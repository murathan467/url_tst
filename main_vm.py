import os
import sys
import signal
import logging
import asyncio

from config import MAX_WORKERS, BATCH_SIZE, TARGET_BASE_URL, TARGET_LIST_URL, BLOCKED_SELLERS
from database import init_db, get_work_batch, is_bargain_notified, mark_bargain_notified, log_skipped_bargain
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
        file_handler = logging.FileHandler("bot_vm.log", encoding="utf-8")
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
    print("\n🛑 Bot (VM) durduruluyor...")
    running = False
    os._exit(0)


async def dispatcher_task(queue: asyncio.Queue):
    """
    İşçiler için Supabase'den RPC ile iş çeker. 
    Aynı anda alınan iş db'den silineceği için yerel makine ile çakışma olmaz.
    """
    logger.info("[Dispatcher] Supabase kuyruk çekicisi başlatıldı.")
    
    # Çok agresif sorgu yapmamak için bekleme süresi
    fetch_interval = 10
    
    while running:
        if queue.qsize() < 3: # Eğer kendi yerel işçi kuyruğumuzda az iş varsa
            try:
                # 30 adet limitli çekiyoruz (worker başı yük)
                batch = get_work_batch(BATCH_SIZE * MAX_WORKERS)
                if batch:
                    logger.info(f"[Dispatcher] Veritabanından {len(batch)} adet ürün işlenmek üzere çekildi.")
                    # Batch'leri ayarlayarak kuyruğa (Queue) verelim
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
            except Exception as e:
                logger.error(f"[Dispatcher] İş çekme hatası: {e}")
                
        for _ in range(fetch_interval):
            if not running: break
            await asyncio.sleep(1)


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
                             send_scan_error_message(bargain.get("skin_name", "Bilinmeyen Skin"))
                             continue
                             
                         seller_lower = bargain["seller"].strip().lower()
                         if seller_lower in BLOCKED_SELLERS:
                             logger.info(f"⛔ Engelli satıcı atlandı: {bargain['seller']}")
                             continue
                         
                         if is_bargain_notified(bargain["skin_name"], bargain["seller"], bargain.get("wear_value", "")):
                             logger.info(f"⏭️ Zaten bildirildi, atlanıyor: {bargain['skin_name']} (Satıcı: {bargain['seller']}, Float: {bargain.get('wear_value', '')})")
                             log_skipped_bargain(bargain["skin_name"], bargain["seller"], bargain.get("wear_value", ""))
                             continue
                             
                         send_notification(
                             skin_name=bargain["skin_name"],
                             seller=bargain["seller"],
                             link=bargain["link"],
                             wear_value=bargain.get("wear_value", ""),
                             is_stattrak=bargain.get("is_stattrak", False)
                         )
                         mark_bargain_notified(bargain["skin_name"], bargain["seller"], bargain.get("wear_value", ""))
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
    logger.info("  VM BOT (YALNIZCA WORKER) BAŞLATILIYOR")
    logger.info("=" * 60)
    
    try:
        init_db()
        # Not: VM tarafında veritabanı temizlenmez (clear_all_skins yok), çünkü Monitor zaten eklemeye devam edebilir.
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
    tasks = []
    
    # Monitor YOK.
    
    # 1. İşleri Supabase'den çekip kuyruğa ekleyen dağıtıcı
    dispatcher = asyncio.create_task(dispatcher_task(queue))
    tasks.append(dispatcher)
    
    # 2. İlanların içine tek tek giren işçiler
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
