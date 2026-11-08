import os
import sys
import time
import random
import signal
import logging
import asyncio
from datetime import datetime

from config import SCAN_INTERVAL_MIN, SCAN_INTERVAL_MAX, TARGET_LIST_URL
from database import init_db, save_skin, save_skins_batch, get_all_skin_ids, clear_all_skins, get_last_scanned_id, set_last_scanned_id
from scraper import DataScraper
from telegram_bot import send_startup_message

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

    try:
        file_handler = logging.FileHandler("bot_monitor.log", encoding="utf-8")
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
    print("\n🛑 Bot (Monitor) durduruluyor...")
    running = False
    from database import clear_all_skins
    try:
        clear_all_skins()
    except Exception:
        pass
    os._exit(0)

async def monitor_task(scraper: DataScraper):
    """
    Yerel makinede ana market sayfasını tarayıp yeni ürünleri Supabase'e ekler.
    Artık işçi konteynerler (Docker) ile veritabanı üzerinden iletişim kurulur.
    """
    monitor_page = await scraper.context.new_page()
    logger.info("[Monitor] Yeni sekme oluşturuldu. Tarama başlatılıyor...")
    
    cycle_count = 0
    
    while running:
        cycle_count += 1
        logger.info("=" * 60)
        logger.info(f" YENİ TARAMA DÖNGÜSÜ #{cycle_count} — {datetime.now().strftime('%H:%M:%S')}")
        logger.info("=" * 60)
        
        try:
            current_last_scanned_id = get_last_scanned_id()
            skins, new_first_id, new_first_name = await scraper.fetch_skin_list(monitor_page, current_last_scanned_id)
            
            if new_first_id and new_first_id != current_last_scanned_id:
                set_last_scanned_id(new_first_id, new_first_name)
                
            if not skins:
                logger.warning("[Monitor] Taranan listede tamamen aynı eşyalar mevcuttu veya hiç skin yoktu.")
            else:
                existing_ids = get_all_skin_ids()
                new_skins = [s for s in skins if s["id"] not in existing_ids]
                
                if new_skins:
                    logger.info(f"🆕 {len(new_skins)} yeni skin bulundu! Buluta (Supabase) aktarılıyor...")
                    batch_data = [{"skin_id": s["id"], "skin_name": s["name"], "wear_value": s.get("wear_value", "")} for s in new_skins]
                    save_skins_batch(batch_data)
                else:
                    logger.info("[Monitor] Yeni skin bulunamadı. Listelenen tüm skinler bulutta bekliyor veya işlenmiş.")
                    
        except Exception as e:
            logger.error(f"[Monitor] Döngü hatası: {e}")
            
        sleep_time = random.uniform(SCAN_INTERVAL_MIN, SCAN_INTERVAL_MAX)
        logger.info(f"[Monitor] Sonraki tarama {sleep_time:.1f} saniye sonra...")
        # Küçük sleep'lerle running kontrolü yapmak için:
        iterations = int(sleep_time * 10)
        for _ in range(iterations):
            if not running: break
            await asyncio.sleep(0.1)
            
    try:
        if not monitor_page.is_closed():
            await monitor_page.close()
    except Exception:
        pass


async def main_async():
    setup_logging()
    logger.info("=" * 60)
    logger.info("  HOST BOT (SADECE MONITOR) BAŞLATILIYOR")
    logger.info("=" * 60)
    
    try:
        init_db()
        clear_all_skins()  # Başlangıçta veritabanını temizle
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
    logger.info("Facebook Login aşaması bitti. Monitor görevi başlatılıyor...")
    
    tasks = []
    
    # 1. Sadece Monitor var
    monitor = asyncio.create_task(monitor_task(scraper))
    tasks.append(monitor)
        
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Ana döngüde beklenmedik hata: {e}")
    finally:
        logger.info("Tarayıcı ve bağlam kapatılıyor...")
        await scraper.close_browser()
        clear_all_skins()  # Kapanışta DB temizle
        logger.info("Monitor Bot başarıyla kapatıldı. 👋")


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
