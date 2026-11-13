import asyncio
import json
import logging
import os
import random
import signal
import sys
import time

try:
    import websockets
except ImportError:
    websockets = None

from config import (
    BATCH_SIZE,
    BLOCKED_SELLERS,
    DISPATCHER_BACKOFF_STEPS,
    MAX_WORKERS,
    TARGET_BASE_URL,
    TARGET_LIST_URL,
    WS_AUTH_TOKEN,
    WS_ENABLED,
    WS_MAX_RETRIES,
    WS_RECONNECT_PROBE_INTERVAL,
    WS_RETRY_DELAYS,
    WS_STARTUP_EMPTY_WAIT,
)
from database import (
    get_work_batch,
    get_ws_url,
    init_db,
    is_bargain_notified,
    log_skipped_bargain,
    mark_bargain_notified,
)
from scraper import DataScraper
from telegram_bot import send_notification, send_scan_error_message, send_startup_message

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
_sig_count = 0
busy_workers = 0
ws_active = False
worker_done_event = asyncio.Event()
fallback_wake_event = asyncio.Event()


def signal_handler(sig, frame):
    global running, _sig_count
    _sig_count += 1
    if _sig_count == 1:
        print("\n🛑 Bot (VM) güvenli şekilde durduruluyor...")
        running = False
    else:
        print("\n🛑 İkinci durdurma sinyali alındı, hemen çıkılıyor...")
        os._exit(0)


async def _sleep_interruptible(seconds: float):
    end_ts = time.time() + seconds
    while running and time.time() < end_ts:
        await asyncio.sleep(min(0.5, max(0.05, end_ts - time.time())))


async def _try_ws_connect_up_to_4_times(node_name: str):
    """system_state'ten ws_url okuyup en fazla 4 kez WebSocket bağlantısı dener."""
    for attempt in range(1, WS_MAX_RETRIES + 1):
        if not running:
            return None

        ws_url = await asyncio.to_thread(get_ws_url)
        if ws_url and (ws_url.startswith("wss://") or ws_url.startswith("ws://")):
            try:
                ws = await websockets.connect(
                    ws_url,
                    open_timeout=10,
                    ping_interval=25,
                    ping_timeout=15,
                )
                auth_payload = {
                    "type": "auth",
                    "token": WS_AUTH_TOKEN,
                    "node": node_name,
                    "max_workers": MAX_WORKERS,
                }
                await ws.send(json.dumps(auth_payload))
                logger.info(
                    f"✅ [WS Client] Tünel bağlantısı kuruldu (Deneme {attempt}/{WS_MAX_RETRIES}): {ws_url}"
                )
                return ws
            except Exception as e:
                logger.warning(
                    f"⚠️ [WS Client] Bağlantı denemesi {attempt}/{WS_MAX_RETRIES} başarısız ({e})."
                )
        else:
            logger.warning(
                f"⚠️ [WS Client] Deneme {attempt}/{WS_MAX_RETRIES}: Geçerli ws_url bulunamadı ({ws_url or 'boş'})."
            )

        if attempt < WS_MAX_RETRIES and running:
            delay = WS_RETRY_DELAYS[min(attempt - 1, len(WS_RETRY_DELAYS) - 1)]
            await _sleep_interruptible(delay)

    return None


async def _wait_for_initial_ws_url(max_wait: float = WS_STARTUP_EMPTY_WAIT):
    """İlk açılışta Master Node tüneli henüz yayınlamadıysa boş ws_url yüzünden 4 deneme hakkını harcamaz."""
    deadline = time.time() + max_wait
    while running and time.time() < deadline:
        ws_url = await asyncio.to_thread(get_ws_url)
        if ws_url and (ws_url.startswith("wss://") or ws_url.startswith("ws://")):
            # Yeni trycloudflare.com alt alan adının DNS yayılımı için kısa bekleme
            await _sleep_interruptible(3.0)
            return
        logger.info("[WS Client] Master tünel adresi bekleniyor (ws_url henüz boş)...")
        await _sleep_interruptible(min(6.0, max(0.5, deadline - time.time())))


async def ws_client_task(queue: asyncio.Queue, node_name: str):
    """
    Birincil WebSocket alıcısı. En fazla 4 denemede bağlanamazsa veya tünel koparsa
    kesintisiz Supabase Fallback moduna geçer ve her 10 dakikada bir tüneli tekrar yoklar.
    """
    global ws_active

    if not WS_ENABLED or websockets is None:
        ws_active = False
        fallback_wake_event.set()
        logger.info("[WS Client] WebSocket devre dışı veya paket yok; Supabase modunda çalışılacak.")
        return

    first_boot = True
    while running:
        if first_boot:
            await _wait_for_initial_ws_url()
            first_boot = False

        ws = await _try_ws_connect_up_to_4_times(node_name)
        if ws is not None:
            ws_active = True
            fallback_wake_event.set()
            try:
                async for raw_msg in ws:
                    if not running:
                        break
                    try:
                        data = json.loads(raw_msg)
                    except Exception:
                        continue

                    if data.get("type") == "skins":
                        items = data.get("items", [])
                        if items:
                            formatted = []
                            for s in items:
                                s_id = s.get("id") or s.get("skin_id")
                                if not s_id:
                                    continue
                                formatted.append({
                                    "id": s_id,
                                    "name": s.get("name") or s.get("skin_name") or f"Skin_{s_id}",
                                    "wear_value": s.get("wear_value", ""),
                                    "url": s.get("url") or f"{TARGET_BASE_URL}/market/goods/{s_id}?game=csgo",
                                })
                            for i in range(0, len(formatted), BATCH_SIZE):
                                await queue.put(formatted[i:i + BATCH_SIZE])
                            logger.info(f"⚡ [WS Client] Tünelden {len(formatted)} adet taze skin alındı.")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"🔌 [WS Client] Tünel bağlantısı koptu ({e}).")
            finally:
                ws_active = False
                fallback_wake_event.set()
                try:
                    await ws.close()
                except Exception:
                    pass

            if running:
                logger.info("🔌 [WS Client] 4 denemeli hızlı yeniden bağlanma başlatılıyor...")
                continue

        # 4 denemede bağlantı kurulamadı -> Kesintisiz Supabase Fallback Modu
        ws_active = False
        fallback_wake_event.set()
        worker_done_event.set()
        logger.warning(
            f"🔄 [Fallback] {WS_MAX_RETRIES} denemede WebSocket kurulamadı. "
            f"Bot duraksamadan Supabase Fallback moduna geçti! ({int(WS_RECONNECT_PROBE_INTERVAL // 60)} dk sonra tünel tekrar yoklanacak)"
        )

        await _sleep_interruptible(WS_RECONNECT_PROBE_INTERVAL)
        if running:
            logger.info("🔍 [Fallback -> WS Probe] 10 dakika doldu, system_state.ws_url tekrar yoklanıyor...")


async def dispatcher_task(queue: asyncio.Queue):
    """
    Fallback modunda MAX_WORKERS - busy_workers - queue.qsize() formülüyle
    yalnızca boşta işçi varken Supabase'den veri çeker (2.5 -> 4.0 -> 6.0 -> 9.0s + jitter).
    """
    logger.info("[Dispatcher] Akıllı Batch Dispatcher başlatıldı (2.5/4.0/6.0/9.0s + jitter, işçi bitiş uyandırmalı).")
    
    backoff_steps = DISPATCHER_BACKOFF_STEPS
    empty_streak = 0
    last_empty_fetch_ts = 0.0
    fetch_interval = backoff_steps[0]
    
    while running:
        if ws_active:
            fallback_wake_event.clear()
            try:
                await asyncio.wait_for(fallback_wake_event.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
            continue

        needed = MAX_WORKERS - busy_workers - queue.qsize()
        if needed > 0:
            if empty_streak > 0 and (time.time() - last_empty_fetch_ts) < 1.5:
                await asyncio.sleep(0.5)
                continue
            try:
                pull_limit = needed * BATCH_SIZE
                batch = await asyncio.to_thread(get_work_batch, pull_limit)
                if batch:
                    empty_streak = 0
                    fetch_interval = backoff_steps[0] + random.uniform(0.1, 0.4)
                    logger.info(f"[Dispatcher] Veritabanından {len(batch)} adet en güncel ürün çekildi (Boş İşçi İhtiyacı: {needed}).")
                    for i in range(0, len(batch), BATCH_SIZE):
                        slice_items = batch[i:i + BATCH_SIZE]
                        worker_items = [{
                            "id": s["skin_id"],
                            "name": s["skin_name"],
                            "wear_value": s.get("wear_value", ""),
                            "url": f"{TARGET_BASE_URL}/market/goods/{s['skin_id']}?game=csgo"
                        } for s in slice_items]
                        await queue.put(worker_items)
                else:
                    last_empty_fetch_ts = time.time()
                    base_step = backoff_steps[min(empty_streak, len(backoff_steps) - 1)]
                    fetch_interval = base_step + random.uniform(0.15, 0.65)
                    empty_streak += 1
                    logger.info(f"[Dispatcher] Kuyruk boş (Deneme #{empty_streak}). {fetch_interval:.1f} sn bekleniyor...")
            except Exception as e:
                logger.error(f"[Dispatcher] İş çekme hatası: {e}")
                fetch_interval = backoff_steps[-1]
        else:
            fetch_interval = backoff_steps[0]
                
        worker_done_event.clear()
        try:
            await asyncio.wait_for(worker_done_event.wait(), timeout=fetch_interval)
        except asyncio.TimeoutError:
            pass


async def worker_task(worker_id: int, scraper: DataScraper, queue: asyncio.Queue):
    global busy_workers
    logger.info(f"[Worker-{worker_id}] Çalışmaya hazır, iş bekliyor...")
    
    while running:
        try:
            batch = await asyncio.wait_for(queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            break
            
        busy_workers += 1
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
             busy_workers = max(0, busy_workers - 1)
             queue.task_done()
             worker_done_event.set()
             logger.info(f"[Worker-{worker_id}] İşini bitirdi, sekmesini kapattı, yeni iş bekliyor...")


async def main_async():
    setup_logging()
    logger.info("=" * 60)
    logger.info("  VM BOT (YALNIZCA WORKER - WS + FALLBACK) BAŞLATILIYOR")
    logger.info("=" * 60)
    
    try:
        await asyncio.to_thread(init_db)
        # Not: VM tarafında veritabanı temizlenmez (clear_all_skins yok), çünkü Monitor zaten eklemeye devam edebilir.
    except Exception as e:
        logger.critical(f"Veritabanı başlatılamadı: {e}")
        sys.exit(1)
        
    node_name = os.getenv("NODE_NAME", "Worker Node")
    await asyncio.to_thread(send_startup_message, node_name)
    
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
    logger.info("Facebook Login aşaması bitti. WebSocket ve Fallback görevleri başlatılıyor...")
    
    queue = asyncio.Queue()
    tasks = []
    
    # 1. Birincil WebSocket alıcısı (4 denemede kurulamazsa veya koparsa Supabase Fallback'e geçer)
    ws_client = asyncio.create_task(ws_client_task(queue, node_name))
    tasks.append(ws_client)
    
    # 2. Fallback modunda Supabase'den akıllı batch çeken dağıtıcı
    dispatcher = asyncio.create_task(dispatcher_task(queue))
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
        for t in tasks:
            if not t.done():
                t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
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
