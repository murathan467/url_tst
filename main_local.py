import os
import sys
import time
import random
import signal
import logging
import asyncio
from datetime import datetime

from config import MAX_WORKERS, BATCH_SIZE, SCAN_INTERVAL_MIN, SCAN_INTERVAL_MAX, TARGET_BASE_URL, TARGET_LIST_URL, BLOCKED_SELLERS
from database import init_db, save_skin, save_skins_batch, get_all_skin_ids, get_work_batch, is_bargain_notified, mark_bargain_notified, clear_all_skins, log_skipped_bargain, get_last_scanned_id, set_last_scanned_id, trim_old_skins
from scraper import DataScraper
from telegram_bot import send_notification, send_startup_message, send_scan_error_message

# ========================
# Logging Ayarları
# ========================
def setup_logging():
    if hasattr(sys.stdout, 'reconfigure'):
        try:
            sys.stdout.reconfigure(encoding='utf-8')  # type: ignore
        except Exception:
            pass
            
    log_format = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format, date_format))
    root_logger.addHandler(console_handler)

    try:
        file_handler = logging.FileHandler("bot_local.log", encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter(log_format, date_format))
        root_logger.addHandler(file_handler)
    except Exception as e:
        print(f"Log dosyası oluşturulamadı: {e}")

    logging.getLogger("urllib3").setLevel(logging.ERROR)
    logging.getLogger("playwright").setLevel(logging.WARNING)


logger = logging.getLogger(__name__)

running = True

def signal_handler(sig, frame):
    global running
    print("\n🛑 Bot (Local) durduruluyor...")
    running = False
    os._exit(0)

async def monitor_task(scraper: DataScraper, new_work_event: asyncio.Event):
    """
    Yerel makinede ana market sayfasını tarayıp yeni ürünleri Supabase'e ekler.
    Kuyruk dağıtımı artık Supabase üzerinden olduğu için sadece DB'ye kaydeder.
    """
    monitor_page = await scraper.context.new_page()
    logger.info("[Monitor] Yeni sekme oluşturuldu. Tarama başlatılıyor...")
    
    cycle_count = 0
    
    while running:
        cycle_count += 1
        logger.info("=" * 60)
        logger.info(f" YENİ TARAMA DÖNGÜSÜ #{cycle_count} — {datetime.now().strftime('%H:%M:%S')}")
        logger.info("=" * 60)
        
        db_congested = False
        try:
            # Her 10 döngüde bir tablo 5.000 satırı aştıysa en eski 4.000 satırı temizle
            if cycle_count % 10 == 0:
                await asyncio.to_thread(trim_old_skins, 5000, 4000)

            current_last_scanned_id = await asyncio.to_thread(get_last_scanned_id)
            skins, new_first_id, new_first_name = await scraper.fetch_skin_list(monitor_page, current_last_scanned_id)
            
            if not skins:
                if new_first_id and new_first_id != current_last_scanned_id:
                    await asyncio.to_thread(set_last_scanned_id, new_first_id, new_first_name)
                logger.warning("[Monitor] Taranan listede tamamen aynı eşyalar mevcuttu veya hiç skin yoktu.")
            else:
                existing_ids = await asyncio.to_thread(get_all_skin_ids)
                new_skins = [s for s in skins if s["id"] not in existing_ids]
                
                if new_skins:
                    logger.info(f"🆕 {len(new_skins)} yeni skin bulundu! Buluta (Supabase) aktarılıyor...")
                    batch_data = [{"skin_id": s["id"], "skin_name": s["name"], "wear_value": s.get("wear_value", "")} for s in new_skins]
                    saved_ok = await asyncio.to_thread(save_skins_batch, batch_data)
                    if saved_ok:
                        if new_first_id and new_first_id != current_last_scanned_id:
                            await asyncio.to_thread(set_last_scanned_id, new_first_id, new_first_name)
                        new_work_event.set()
                    else:
                        db_congested = True
                        logger.warning("[Monitor] Skinler buluta kaydedilemediği için 'last_scanned_id' güncellenmedi; veritabanı rahatlaması için ekstra beklenecek.")
                else:
                    if new_first_id and new_first_id != current_last_scanned_id:
                        await asyncio.to_thread(set_last_scanned_id, new_first_id, new_first_name)
                    logger.info("[Monitor] Yeni skin bulunamadı. Listelenen tüm skinler bulutta bekliyor veya işlenmiş.")
                    
        except Exception as e:
            logger.error(f"[Monitor] Döngü hatası: {e}")
            
        if db_congested:
            sleep_time = random.uniform(25.0, 35.0)
            logger.warning(f"[Monitor] Veritabanı yoğunluğu (Backoff): {sleep_time:.1f} saniye bekleniyor...")
        else:
            sleep_time = random.uniform(SCAN_INTERVAL_MIN, SCAN_INTERVAL_MAX)
            logger.info(f"[Monitor] Sonraki tarama {sleep_time:.1f} saniye sonra...")
        iterations = int(sleep_time * 10)
        for _ in range(iterations):
            if not running: break
            await asyncio.sleep(0.1)
            
    try:
        if not monitor_page.is_closed():
            await monitor_page.close()
    except Exception:
        pass


async def dispatcher_task(queue: asyncio.Queue, new_work_event: asyncio.Event):
    """
    İşçiler için Supabase'den RPC ile iş çeker (Adaptive Backoff + Event destekli).
    Aynı anda alınan iş db'den silineceği için VM ile çakışma olmaz.
    """
    logger.info("[Dispatcher] Supabase kuyruk çekicisi başlatıldı.")
    
    fetch_interval = 4.0
    pull_limit = max(BATCH_SIZE * MAX_WORKERS, 5)
    
    while running:
        if queue.qsize() < 3:
            try:
                batch = await asyncio.to_thread(get_work_batch, pull_limit)
                if batch:
                    fetch_interval = 4.0
                    logger.info(f"[Dispatcher] Veritabanından {len(batch)} adet ürün işlenmek üzere çekildi.")
                    for i in range(0, len(batch), BATCH_SIZE):
                        slice_items = batch[i:i + BATCH_SIZE]
                        worker_items = []
                        for s in slice_items:
                            worker_items.append({
                                "id": s["skin_id"],
                                "name": s["skin_name"],
                                "wear_value": s.get("wear_value", ""),
                                "url": f"{TARGET_BASE_URL}/market/goods/{s['skin_id']}?game=csgo"
                            })
                        await queue.put(worker_items)
                else:
                    # Kuyruk boşsa bekleme süresini kademeli olarak 12 saniyeye kadar çıkar
                    fetch_interval = min(fetch_interval + 2.0, 12.0)
            except Exception as e:
                logger.error(f"[Dispatcher] İş çekme hatası: {e}")
                fetch_interval = 10.0
                
        # Boş bekleyişlerde Monitor yeni iş eklerse anında uyan
        new_work_event.clear()
        try:
            await asyncio.wait_for(new_work_event.wait(), timeout=fetch_interval)
            fetch_interval = 2.0
        except asyncio.TimeoutError:
            pass


async def worker_task(worker_id: int, scraper: DataScraper, queue: asyncio.Queue):
    logger.info(f"[Worker-{worker_id}] Çalışmaya hazır, iş bekliyor...")
    
    while running:
        try:
            batch = await asyncio.wait_for(queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            break
            
        logger.info(f"[Worker-{worker_id}] {len(batch)} adetlik yeni iş paketi aldı. Yeni sekme açılıyor...")
        
        worker_page = None
        try:
             worker_page = await scraper.context.new_page()
             await worker_page.set_viewport_size({"width": 1920, "height": 1080})
             total_bargains = 0
             
             for i, skin in enumerate(batch, 1):
                 if not running: break
                 logger.info(f"[Worker-{worker_id}] [{i}/{len(batch)}] İnceleniyor: {skin['name']}")
                 
                 try:
                     bargain_items = await scraper.check_detail_page_for_bargain(
                         page=worker_page,
                         skin_url=skin["url"],
                         skin_name=skin["name"]
                     )
                     
                     for bargain in bargain_items:
                         if bargain.get("error") == "sort_failed":
                             await asyncio.to_thread(send_scan_error_message, bargain.get("skin_name", "Bilinmeyen Skin"))
                             continue
                             
                         seller_lower = bargain["seller"].strip().lower()
                         if seller_lower in BLOCKED_SELLERS:
                             logger.info(f"⛔ Engelli satıcı atlandı: {bargain['seller']}")
                             continue
                         
                         already_notified = await asyncio.to_thread(
                             is_bargain_notified,
                             bargain["skin_name"],
                             bargain["seller"],
                             bargain.get("wear_value", "")
                         )
                         if already_notified:
                             logger.info(f"⏭️ Zaten bildirildi, atlanıyor: {bargain['skin_name']} (Satıcı: {bargain['seller']}, Float: {bargain.get('wear_value', '')})")
                             await asyncio.to_thread(log_skipped_bargain, bargain["skin_name"], bargain["seller"], bargain.get("wear_value", ""))
                             continue
                             
                         await asyncio.to_thread(
                             send_notification,
                             skin_name=bargain["skin_name"],
                             seller=bargain["seller"],
                             link=bargain["link"],
                             wear_value=bargain.get("wear_value", ""),
                             is_stattrak=bargain.get("is_stattrak", False)
                         )
                         await asyncio.to_thread(
                             mark_bargain_notified,
                             bargain["skin_name"],
                             bargain["seller"],
                             bargain.get("wear_value", "")
                         )
                         total_bargains += 1
                         
                 except Exception as e:
                     logger.error(f"[Worker-{worker_id}] Skin işleme hatası ({skin['name']}): {e}")
                     
             logger.info(f"[Worker-{worker_id}] Paket tamamlandı. Toplam {total_bargains} bargain bildirildi.")
                     
        except Exception as e:
             logger.error(f"[Worker-{worker_id}] Genel paket işleme hatası: {e}")
        finally:
             if worker_page and not worker_page.is_closed():
                 try:
                     await worker_page.close()
                 except Exception:
                     pass
             queue.task_done()
             logger.info(f"[Worker-{worker_id}] İşini bitirdi, sekmesini kapattı, yeni iş bekliyor...")


async def main_async():
    setup_logging()
    logger.info("=" * 60)
    logger.info("  LOCAL BOT (MONITOR + WORKER) BAŞLATILIYOR")
    logger.info("=" * 60)
    
    try:
        init_db()
    except Exception as e:
        logger.critical(f"Veritabanı başlatılamadı: {e}")
        sys.exit(1)
        
    send_startup_message()
    
    scraper = DataScraper()
    try:
        await scraper.start_browser()
    except Exception as e:
        logger.critical(f"Tarayıcı başlatılamadı: {e}")
        sys.exit(1)
        
    logger.info("Oturum doğrulaması için sekme açılıyor...")
    login_page = await scraper.context.new_page()
    await login_page.set_viewport_size({"width": 1920, "height": 1080})
    
    try:
        await login_page.goto(TARGET_LIST_URL, timeout=30000)
    except Exception:
         pass
         
    from facebook_auth import handle_facebook_login
    await handle_facebook_login(login_page)
    
    try:
        await login_page.close()
    except Exception:
        pass
    logger.info("Facebook Login aşaması bitti. Görevler dağıtılıyor...")
    
    queue = asyncio.Queue()
    new_work_event = asyncio.Event()
    tasks = []
    
    # 1. Ana sayfayı tarayıp Supabase'e iten monitor
    monitor = asyncio.create_task(monitor_task(scraper, new_work_event))
    tasks.append(monitor)
    
    # 2. İşleri Supabase'den çekip kuyruğa ekleyen dağıtıcı
    dispatcher = asyncio.create_task(dispatcher_task(queue, new_work_event))
    tasks.append(dispatcher)
    
    # 3. İlanların içine tek tek giren işçiler
    for i in range(1, MAX_WORKERS + 1):
        w_task = asyncio.create_task(worker_task(i, scraper, queue))
        tasks.append(w_task)
        
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Ana döngüde beklenmedik hata: {e}")
    finally:
        logger.info("Tarayıcı ve bağlam kapatılıyor...")
        await scraper.close_browser()
        logger.info("Bot başarıyla kapatıldı. Hoşça kalın! 👋")


def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        if sys.platform == 'win32':
             asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
