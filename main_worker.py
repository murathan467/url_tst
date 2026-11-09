import os
import sys
import signal
import logging
import asyncio

from config import BATCH_SIZE, TARGET_BASE_URL, TARGET_LIST_URL, BLOCKED_SELLERS
from database import init_db, get_work_batch, is_bargain_notified, mark_bargain_notified, log_skipped_bargain
from scraper import DataScraper
from telegram_bot import send_notification, send_scan_error_message
from proxy_manager import get_free_proxies, get_working_proxy

def setup_logging():
    if hasattr(sys.stdout, 'reconfigure'):
        try:
            sys.stdout.reconfigure(encoding='utf-8')
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

    logging.getLogger("urllib3").setLevel(logging.ERROR)
    logging.getLogger("playwright").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

running = True

def signal_handler(sig, frame):
    global running
    print("\n🛑 Bot (Worker) durduruluyor...")
    running = False
    os._exit(0)

async def dispatcher_task(queue: asyncio.Queue):
    """Sıradaki işleri Supabase üzerinden akıllı aralıklarla (Adaptive Backoff) çeker."""
    logger.info("[Dispatcher] Supabase kuyruk çekicisi başlatıldı (IO Optimize).")
    
    # Kuyrukta iş varken hızlı, iş yokken yavaşlayan bekleme süresi
    fetch_interval = 3.0
    # Her RPC çağrısında tek tek değil, en az 3 ürün çekerek DB yükünü 3 kat azalt
    pull_limit = max(BATCH_SIZE, 3)
    
    while running:
        if queue.qsize() < 2: 
            try:
                batch = await asyncio.to_thread(get_work_batch, pull_limit)
                if batch:
                    fetch_interval = 3.0  # İş bulundu, aktif moda geç
                    logger.info(f"[Dispatcher] Veritabanından {len(batch)} adet ürün işlenmek üzere çekildi.")
                    for s in batch:
                        await queue.put([{
                            "id": s["skin_id"],
                            "name": s["skin_name"],
                            "wear_value": s.get("wear_value", ""),
                            "url": f"{TARGET_BASE_URL}/market/goods/{s['skin_id']}?game=csgo"
                        }])
                else:
                    # Kuyruk boşsa bekleme süresini kademeli olarak 10 saniyeye kadar çıkar (Disk IO Koruması)
                    fetch_interval = min(fetch_interval + 2.0, 10.0)
            except Exception as e:
                logger.error(f"[Dispatcher] İş çekme hatası: {e}")
                fetch_interval = 8.0
                
        iterations = int(fetch_interval * 10)
        for _ in range(iterations):
            if not running: break
            await asyncio.sleep(0.1)

async def worker_task(worker_id: str, scraper: DataScraper, queue: asyncio.Queue):
    """Sıradaki işleri alır ve proxy ile Playwright sayfasında test eder."""
    logger.info(f"[Worker-{worker_id}] Çalışmaya hazır, iş bekliyor...")
    
    while running:
        try:
            batch = await asyncio.wait_for(queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            break
            
        logger.info(f"[Worker-{worker_id}] Yeni iş paketi alındı. Analiz başlıyor...")
        
        worker_page = None
        try:
             worker_page = await scraper.context.new_page()
             await worker_page.set_viewport_size({"width": 1920, "height": 1080})
             total_bargains = 0
             
             for i, skin in enumerate(batch, 1):
                 if not running: break
                 logger.info(f"[Worker-{worker_id}] İnceleniyor: {skin['name']}")
                 
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
                             logger.info(f"⏭️ Zaten bildirildi, atlanıyor: {bargain['skin_name']} (Satıcı: {bargain['seller']})")
                             log_skipped_bargain(bargain["skin_name"], bargain["seller"], bargain.get("wear_value", ""))
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
                     if "WAF Engeli" in str(e) or "Timeout" in str(e) or "net::ERR_" in str(e):
                         raise e
                     logger.error(f"[Worker-{worker_id}] Skin işleme hatası ({skin['name']}): {e}")
                     
             logger.info(f"[Worker-{worker_id}] Paket tamamlandı. Toplam {total_bargains} bargain bildirildi.")
                     
        except Exception as e:
             logger.error(f"[Worker-{worker_id}] Kritik Hata (Proxy veya WAF): {e}")
             if worker_page and not worker_page.is_closed():
                 try:
                     await worker_page.close()
                 except Exception:
                     pass
             queue.task_done()
             raise e
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
    worker_id = os.environ.get("WORKER_ID", "1")
    
    logger.info("=" * 60)
    logger.info(f"  DOCKER BOT (WORKER {worker_id}) BAŞLATILIYOR (DİNAMİK PROXY)")
    logger.info("=" * 60)
    
    try:
        init_db()
    except Exception as e:
        logger.critical(f"Veritabanı başlatılamadı: {e}")
        sys.exit(1)
        
    queue = asyncio.Queue()
    asyncio.create_task(dispatcher_task(queue))

    proxies = get_free_proxies()
    
    while running:
        working_proxy = get_working_proxy(proxies)
        
        if not working_proxy:
            logger.warning("[ProxyLoop] Çalışan proxy kalmadı, listeyi yeniliyorum...")
            proxies = get_free_proxies()
            await asyncio.sleep(10)
            continue
            
        proxy_url = f"http://{working_proxy}"
        logger.info(f"[ProxyLoop] Worker {worker_id} - Yeni Proxy İle Başlıyor: {proxy_url}")
        
        scraper = DataScraper()
        try:
            await scraper.start_browser(proxy_url=proxy_url)
            
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
                
            logger.info("Worker iş döngüsü başlatılıyor...")
            await worker_task(worker_id, scraper, queue)
            
        except Exception as e:
            logger.warning(f"[ProxyLoop] Proxy patladı veya engellendi ({working_proxy}). Hata: {e}")
            if working_proxy in proxies:
                proxies.remove(working_proxy)
        finally:
            logger.info("[ProxyLoop] Tarayıcı ve bağlam kapatılıyor...")
            await scraper.close_browser()
            await asyncio.sleep(2)


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