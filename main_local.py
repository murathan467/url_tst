import asyncio
from datetime import datetime
import json
import logging
import os
import random
import re
import shutil
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
    SCAN_INTERVAL_MAX,
    SCAN_INTERVAL_MIN,
    TARGET_BASE_URL,
    TARGET_LIST_URL,
    WS_AUTH_TOKEN,
    WS_ENABLED,
    WS_HOST,
    WS_PORT,
)
from database import (
    add_known_skin_ids,
    flush_last_scanned_id,
    get_all_skin_ids,
    get_last_scanned_id,
    get_work_batch,
    init_db,
    is_bargain_notified,
    log_skipped_bargain,
    mark_bargain_notified,
    save_skins_batch,
    set_last_scanned_id,
    set_ws_url,
    trim_old_skins,
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
_sig_count = 0
busy_workers = 0
worker_done_event = asyncio.Event()
ws_clients = []
ws_client_names = {}
ws_rr_index = 0
last_supabase_push_ts = 0.0
cloudflared_proc = None
ws_server = None
_tunnel_drain_tasks = []


def signal_handler(sig, frame):
    global running, _sig_count
    _sig_count += 1
    if _sig_count == 1:
        print("\n🛑 Bot (Local) güvenli şekilde durduruluyor (durum Supabase'e yazılıyor)...")
        running = False
    else:
        print("\n🛑 İkinci durdurma sinyali alındı, hemen çıkılıyor...")
        os._exit(0)


def _format_worker_item(skin: dict) -> dict:
    return {
        "id": skin["id"],
        "name": skin["name"],
        "wear_value": skin.get("wear_value", ""),
        "url": f"{TARGET_BASE_URL}/market/goods/{skin['id']}?game=csgo",
    }


def _remove_ws_client(ws):
    if ws in ws_clients:
        ws_clients.remove(ws)
    name = ws_client_names.pop(ws, "Bilinmeyen Worker")
    return name


async def _ws_client_handler(websocket, *args):
    """Bağlanan Worker botların WebSocket oturumunu doğrular ve canlı tutar."""
    node_name = "Worker"
    try:
        if WS_AUTH_TOKEN:
            try:
                raw_auth = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                auth_msg = json.loads(raw_auth)
            except Exception:
                await websocket.close(code=4001, reason="Auth timeout or invalid payload")
                return

            if auth_msg.get("type") != "auth" or auth_msg.get("token") != WS_AUTH_TOKEN:
                logger.warning("[WS Server] Yetkisiz WebSocket bağlantı denemesi reddedildi.")
                await websocket.close(code=4001, reason="Unauthorized")
                return
            node_name = str(auth_msg.get("node") or "Worker")

        ws_clients.append(websocket)
        ws_client_names[websocket] = node_name
        logger.info(f"🔗 [WS Server] Worker bağlandı: {node_name} (Aktif WS Worker: {len(ws_clients)})")

        async for raw_msg in websocket:
            if not running:
                break
            try:
                msg = json.loads(raw_msg)
                if msg.get("type") == "ping":
                    await websocket.send(json.dumps({"type": "pong"}))
            except Exception:
                pass
    except Exception as e:
        logger.debug(f"[WS Server] İstemci bağlantısı kapandı ({node_name}): {e}")
    finally:
        removed_name = _remove_ws_client(websocket)
        logger.info(f"🔌 [WS Server] Worker ayrıldı: {removed_name} (Kalan WS Worker: {len(ws_clients)})")


async def start_ws_server():
    """Yerel WebSocket sunucusunu başlatır ve dinlediği portu döndürür."""
    global ws_server
    if websockets is None:
        logger.warning("[WS Server] 'websockets' paketi kurulu değil, yalnızca Supabase modu çalışacak.")
        return None
    try:
        ws_server = await websockets.serve(
            _ws_client_handler,
            WS_HOST,
            WS_PORT,
            ping_interval=25,
            ping_timeout=15,
        )
    except OSError:
        ws_server = await websockets.serve(
            _ws_client_handler,
            WS_HOST,
            0,
            ping_interval=25,
            ping_timeout=15,
        )
    actual_port = ws_server.sockets[0].getsockname()[1]
    logger.info(f"🚀 [WS Server] ws://{WS_HOST}:{actual_port} üzerinde başlatıldı.")
    return actual_port


async def _drain_stream(stream, stop_log_after_url: bool = False):
    """Subprocess borusunun dolup cloudflared'ı kilitlemesini önlemek için çıktıyı tüketir."""
    try:
        while True:
            line = await stream.readline()
            if not line:
                break
            if not stop_log_after_url:
                text = line.decode("utf-8", errors="replace").strip()
                if text:
                    logger.debug(f"[cloudflared] {text}")
    except Exception:
        pass


async def _launch_cloudflared_once(port: int) -> str:
    """cloudflared Quick Tunnel başlatır ve wss://...trycloudflare.com adresini yakalar."""
    global cloudflared_proc, _tunnel_drain_tasks
    cf_bin = shutil.which("cloudflared")
    if not cf_bin:
        logger.warning("[WS Tunnel] 'cloudflared' bulunamadı; uzak işçiler için Supabase Fallback kullanılacak.")
        return ""

    cloudflared_proc = await asyncio.create_subprocess_exec(
        cf_bin,
        "tunnel",
        "--no-autoupdate",
        "--url",
        f"http://{WS_HOST}:{port}",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    url_pattern = re.compile(r"https://([a-zA-Z0-9-]+)\.trycloudflare\.com")
    found_url = ""
    deadline = time.time() + 25.0

    async def _read_until_url(stream):
        nonlocal found_url
        while time.time() < deadline and not found_url and running:
            remaining = max(0.5, deadline - time.time())
            try:
                line = await asyncio.wait_for(stream.readline(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            if not line:
                break
            text = line.decode("utf-8", errors="replace").strip()
            match = url_pattern.search(text)
            if match:
                found_url = f"wss://{match.group(1)}.trycloudflare.com"
                break

    reader_tasks = [
        asyncio.create_task(_read_until_url(cloudflared_proc.stderr)),
        asyncio.create_task(_read_until_url(cloudflared_proc.stdout)),
    ]
    while time.time() < deadline and not found_url and running:
        if all(t.done() for t in reader_tasks):
            break
        await asyncio.sleep(0.2)

    for t in reader_tasks:
        if not t.done():
            t.cancel()

    _tunnel_drain_tasks = [
        asyncio.create_task(_drain_stream(cloudflared_proc.stderr, True)),
        asyncio.create_task(_drain_stream(cloudflared_proc.stdout, True)),
    ]

    if not found_url:
        logger.warning("[WS Tunnel] cloudflared 25 sn içinde tünel URL'si üretemedi; Supabase moduna geçiliyor.")
        await _stop_cloudflared()
        return ""

    logger.info(f"🌐 [WS Tunnel] Aktif Cloudflare tünel adresi: {found_url}")
    return found_url


async def _stop_cloudflared():
    global cloudflared_proc, _tunnel_drain_tasks
    for t in _tunnel_drain_tasks:
        if not t.done():
            t.cancel()
    _tunnel_drain_tasks = []
    if cloudflared_proc is not None:
        try:
            if cloudflared_proc.returncode is None:
                cloudflared_proc.terminate()
                try:
                    await asyncio.wait_for(cloudflared_proc.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    cloudflared_proc.kill()
        except Exception:
            pass
        cloudflared_proc = None


async def tunnel_supervisor_task(port: int):
    """cloudflared süreci beklenmedik şekilde düşerse yeniden başlatıp ws_url'yi günceller."""
    while running:
        if cloudflared_proc is None:
            wss_url = await _launch_cloudflared_once(port)
            await asyncio.to_thread(set_ws_url, wss_url)
            if not wss_url:
                for _ in range(120):
                    if not running:
                        return
                    await asyncio.sleep(0.5)
                continue

        try:
            rc = await cloudflared_proc.wait()
        except asyncio.CancelledError:
            break
        except Exception:
            rc = -1

        if not running:
            break
        logger.warning(f"⚠️ [WS Tunnel] cloudflared beklenmedik şekilde kapandı (kod={rc}). 5 sn sonra yeniden başlatılıyor...")
        await _stop_cloudflared()
        await asyncio.to_thread(set_ws_url, "")
        await asyncio.sleep(5.0)


async def dispatch_remote_skins_via_ws(remote_skins: list) -> list:
    """Kalan skinleri bağlı WebSocket worker botlarına round-robin ile dağıtır; gönderilemeyenleri döndürür."""
    global ws_rr_index
    if not ws_clients or not remote_skins:
        return list(remote_skins)

    unsent = []
    sent_ids = []
    for skin in remote_skins:
        delivered = False
        while ws_clients and not delivered:
            idx = ws_rr_index % len(ws_clients)
            ws = ws_clients[idx]
            ws_rr_index += 1
            payload = json.dumps({"type": "skins", "items": [_format_worker_item(skin)]})
            try:
                await ws.send(payload)
                delivered = True
                sent_ids.append(skin["id"])
            except Exception as e:
                dead_name = _remove_ws_client(ws)
                logger.warning(f"[WS Server] Gönderim hatası ({dead_name}), istemci çıkarıldı: {e}")
        if not delivered:
            unsent.append(skin)

    if sent_ids:
        add_known_skin_ids(sent_ids)
    return unsent


async def monitor_task(scraper: DataScraper, queue: asyncio.Queue):
    """
    Yerel makinede ana market sayfasını tarayıp yeni ürünleri önce kendi boş işçilerine,
    kalanını ise WebSocket tünelinden (bağlı bot yoksa Supabase'e) aktarır.
    """
    global last_supabase_push_ts
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
            # Her 50 döngüde bir tablo 150 satırı aştıysa en eski 100 bayat satırı temizle
            if cycle_count % 50 == 0:
                await asyncio.to_thread(trim_old_skins, 150, 100)

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
                    # Önce Master'ın kendi boş yerel işçilerini (queue) DB'ye gitmeden doğrudan besle (0 DB IO)
                    local_need = max(0, MAX_WORKERS - busy_workers - queue.qsize())
                    local_skins = new_skins[:local_need]
                    remote_skins = new_skins[local_need:]

                    if local_skins:
                        add_known_skin_ids([s["id"] for s in local_skins])
                        for i in range(0, len(local_skins), BATCH_SIZE):
                            slice_items = local_skins[i:i + BATCH_SIZE]
                            worker_items = [_format_worker_item(s) for s in slice_items]
                            await queue.put(worker_items)
                        logger.info(f"⚡ [Monitor] {len(local_skins)} taze skin doğrudan yerel işçilere (DB'siz) aktarıldı.")

                    if remote_skins:
                        unsent_skins = await dispatch_remote_skins_via_ws(remote_skins)
                        sent_ws_count = len(remote_skins) - len(unsent_skins)
                        if sent_ws_count > 0:
                            logger.info(f"🌐 [WS Tunnel] {sent_ws_count} yeni skin WebSocket tünelinden Worker botlara iletildi (0 DB IO).")

                        if unsent_skins:
                            logger.info(f"🆕 {len(unsent_skins)} yeni skin diğer Worker'lar için buluta (Supabase Fallback) aktarılıyor...")
                            batch_data = [{"skin_id": s["id"], "skin_name": s["name"], "wear_value": s.get("wear_value", "")} for s in unsent_skins]
                            saved_ok = await asyncio.to_thread(save_skins_batch, batch_data)
                            if saved_ok:
                                last_supabase_push_ts = time.time()
                                if new_first_id and new_first_id != current_last_scanned_id:
                                    await asyncio.to_thread(set_last_scanned_id, new_first_id, new_first_name)
                            else:
                                db_congested = True
                                logger.warning("[Monitor] Skinler buluta kaydedilemediği için 'last_scanned_id' güncellenmedi; veritabanı rahatlaması için ekstra beklenecek.")
                        else:
                            if new_first_id and new_first_id != current_last_scanned_id:
                                await asyncio.to_thread(set_last_scanned_id, new_first_id, new_first_name)
                    else:
                        if new_first_id and new_first_id != current_last_scanned_id:
                            await asyncio.to_thread(set_last_scanned_id, new_first_id, new_first_name)
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


async def dispatcher_task(queue: asyncio.Queue):
    """
    Monitor tetiklemesi olmadan yalnızca yerel işçiler boşaldığında (ve WS istemcisi yokken)
    Supabase'de kalan sahipsiz işleri çeken yedek dağıtıcı.
    """
    logger.info("[Dispatcher] Supabase yedek kuyruk çekicisi başlatıldı (Backoff: 2.5/4.0/6.0/9.0s + jitter).")
    
    backoff_steps = DISPATCHER_BACKOFF_STEPS
    empty_streak = 0
    last_empty_fetch_ts = 0.0
    fetch_interval = backoff_steps[0]
    
    while running:
        needed = MAX_WORKERS - busy_workers - queue.qsize()
        recently_pushed_for_remote = (time.time() - last_supabase_push_ts) < 10.0

        if needed > 0 and not ws_clients and not recently_pushed_for_remote:
            if empty_streak > 0 and (time.time() - last_empty_fetch_ts) < 1.5:
                await asyncio.sleep(0.5)
                continue
            try:
                pull_limit = needed * BATCH_SIZE
                batch = await asyncio.to_thread(get_work_batch, pull_limit)
                if batch:
                    empty_streak = 0
                    fetch_interval = backoff_steps[0] + random.uniform(0.1, 0.4)
                    logger.info(f"[Dispatcher] Veritabanından {len(batch)} adet ürün çekildi (İhtiyaç: {needed}).")
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
                    logger.debug(f"[Dispatcher] Kuyruk boş (Deneme #{empty_streak}). {fetch_interval:.1f} sn bekleniyor...")
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
    logger.info("  LOCAL BOT (MONITOR + WORKER + WS TÜNEL) BAŞLATILIYOR")
    logger.info("=" * 60)
    
    try:
        await asyncio.to_thread(init_db)
    except Exception as e:
        logger.critical(f"Veritabanı başlatılamadı: {e}")
        sys.exit(1)
        
    await asyncio.to_thread(send_startup_message)
    
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
    logger.info("Facebook Login aşaması bitti. WebSocket tüneli ve görevler başlatılıyor...")
    
    queue = asyncio.Queue()
    tasks = []

    if WS_ENABLED:
        actual_port = await start_ws_server()
        if actual_port:
            wss_url = await _launch_cloudflared_once(actual_port)
            await asyncio.to_thread(set_ws_url, wss_url)
            tasks.append(asyncio.create_task(tunnel_supervisor_task(actual_port)))
        else:
            await asyncio.to_thread(set_ws_url, "")
    else:
        await asyncio.to_thread(set_ws_url, "")
    
    # 1. Ana sayfayı tarayıp önce yerel işçileri, fazlasını WebSocket / Supabase üzerinden dağıtan monitor
    monitor = asyncio.create_task(monitor_task(scraper, queue))
    tasks.append(monitor)
    
    # 2. Yalnızca yerel işçiler boş kaldığında yedek çeken dağıtıcı
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
        await _stop_cloudflared()
        if ws_server is not None:
            try:
                ws_server.close()
                await ws_server.wait_closed()
            except Exception:
                pass
        try:
            await asyncio.to_thread(flush_last_scanned_id)
            await asyncio.to_thread(set_ws_url, "")
        except Exception:
            pass
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
