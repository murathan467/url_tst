# -*- coding: utf-8 -*-
"""
Cloud Task Sync — Veritabanı Modülü (Supabase)
Optimize Edilmiş Versiyon: RAM Cache, Upsert Ignore Duplicates, Otomatik Bağlantı Yenileme ve Sıfıra Yakın Disk IO.
"""

import time
import logging
import threading
import asyncio
from datetime import datetime
from supabase import create_client, Client, ClientOptions

from config import SUPABASE_URL, SUPABASE_KEY
import httpx

# httpx seviyesinde HTTP/2'yi kapat, stale keep-alive soketlerini önle ve sıkı timeout uygula
_original_client_init = httpx.Client.__init__
def _patched_client_init(self, *args, **kwargs):
    kwargs['http2'] = False
    kwargs['timeout'] = httpx.Timeout(12.0, connect=5.0, read=10.0, write=10.0, pool=5.0)
    kwargs['limits'] = httpx.Limits(max_connections=10, max_keepalive_connections=0)
    headers = dict(kwargs.get('headers') or {})
    headers['Connection'] = 'close'
    kwargs['headers'] = headers
    _original_client_init(self, *args, **kwargs)
httpx.Client.__init__ = _patched_client_init

logger = logging.getLogger(__name__)

supabase: Client = None
_db_lock = threading.RLock()

# Eşzamanlı (Concurrent) İstekleri Sınırlandırmak için Semaphore
db_semaphore = asyncio.Semaphore(5)

# ==========================================================
# DİSK IO OPTİMİZASYONU: BELLEK İÇİ ÖNBELLEKLER (RAM CACHE)
# ==========================================================
_notified_cache = set()          # (skin_name, seller, float_value)
_known_skin_ids_cache = set()    # Bilinen skin_id'ler
_skin_ids_cache_loaded = False
_last_scanned_id_cache = None    # Son taranan skin_id
_last_state_sync_time = 0.0      # system_state tablosuna son yazma zamanı (Throttling)
_skin_insert_buffer = []         # Tekli save_skin çağrıları için tampon


def _create_supabase_client() -> Client:
    """Yeni bir Supabase istemcisi oluşturur ve eski oturumu güvenli şekilde kapatır."""
    global supabase
    with _db_lock:
        if supabase is not None:
            try:
                if hasattr(supabase, 'postgrest') and hasattr(supabase.postgrest, 'session'):
                    supabase.postgrest.session.close()
            except Exception:
                pass

        options = ClientOptions(postgrest_client_timeout=10)
        client = create_client(SUPABASE_URL, SUPABASE_KEY, options=options)

        if hasattr(client, 'postgrest') and hasattr(client.postgrest, 'client'):
            try:
                client.postgrest.client._http2 = False
            except Exception:
                pass

        supabase = client
        return supabase


def _is_recoverable_network_error(err: Exception) -> bool:
    """Timeout, kopan soket veya geçici ağ hatalarını tespit eder."""
    msg = str(err).lower()
    keywords = (
        "timed out",
        "timeout",
        "connection",
        "disconnected",
        "remoteprotocolerror",
        "readerror",
        "writeerror",
        "connecterror",
        "pooltimeout",
        "eof",
        "reset by peer",
        "broken pipe",
        "502",
        "503",
        "504",
    )
    return any(k in msg for k in keywords)


def _execute_with_retry(operation_fn, action_name: str = "DB İşlemi", retries: int = 2):
    """
    Supabase sorgularını thread-safe olarak çalıştırır.
    ReadTimeout veya soket takılması durumunda istemciyi sıfırlayıp tekrar dener.
    """
    global supabase
    last_err = None
    for attempt in range(retries + 1):
        try:
            with _db_lock:
                if not supabase:
                    _create_supabase_client()
                return operation_fn(supabase)
        except Exception as e:
            last_err = e
            if _is_recoverable_network_error(e) and attempt < retries:
                logger.warning(
                    f"[DB Retry] '{action_name}' sırasında ağ/zaman aşımı ({e}). "
                    f"Bağlantı yenileniyor (Deneme {attempt + 1}/{retries})..."
                )
                with _db_lock:
                    try:
                        _create_supabase_client()
                    except Exception:
                        pass
                time.sleep(0.8 * (attempt + 1))
            else:
                break
    raise last_err


def init_db():
    global supabase
    try:
        _create_supabase_client()
        logger.info("Supabase bağlantısı başarıyla başlatıldı.")
        _preload_notified_cache()
    except Exception as e:
        logger.error(f"Supabase başlatma hatası: {e}")
        raise


def _preload_notified_cache():
    """Başlangıçta son 1000 bildirimi RAM'e çeker, tekrarlayan SELECT sorgularını önler."""
    global _notified_cache
    try:
        res = _execute_with_retry(
            lambda client: client.table("notified_bargains")
            .select("skin_name, seller, float_value")
            .order("id", desc=True)
            .limit(1000)
            .execute(),
            action_name="Preload Notified Cache",
            retries=1,
        )
        if res.data:
            for row in res.data:
                key = (row.get("skin_name", ""), row.get("seller", ""), row.get("float_value", ""))
                _notified_cache.add(key)
            logger.info(f"[RAM Cache] {len(_notified_cache)} adet geçmiş bildirim önbelleğe yüklendi.")
    except Exception as e:
        logger.debug(f"Önbellek yükleme uyarısı: {e}")


def is_skin_exists(skin_id: str) -> bool:
    if skin_id in _known_skin_ids_cache:
        return True
    try:
        response = _execute_with_retry(
            lambda client: client.table("skins").select("id").eq("skin_id", skin_id).limit(1).execute(),
            action_name="is_skin_exists",
            retries=1,
        )
        exists = len(response.data) > 0
        if exists:
            _known_skin_ids_cache.add(skin_id)
        return exists
    except Exception as e:
        logger.error(f"Skin kontrolü hatası (skin_id={skin_id}): {e}")
        return False


def save_skin(skin_id: str, skin_name: str = "", wear_value: str = ""):
    """Tekli çağrıları tamponda biriktirip 20'şerli toplu yazar."""
    if skin_id in _known_skin_ids_cache:
        return
    _skin_insert_buffer.append({
        "skin_id": skin_id,
        "skin_name": skin_name,
        "wear_value": wear_value
    })
    if len(_skin_insert_buffer) >= 20:
        flush_skin_buffer()


def flush_skin_buffer():
    global _skin_insert_buffer
    if not _skin_insert_buffer:
        return
    to_save = _skin_insert_buffer[:]
    _skin_insert_buffer.clear()
    save_skins_batch(to_save)


def save_skins_batch(skins_list: list) -> bool:
    """
    Toplu Ekleme (Bulk Upsert) - Çakışan (Duplicate) kayıtları sessizce yoksayar,
    sadece yeni olanları tek bir HTTP isteği ve tek bir Disk IO işlemiyle yazar.
    Başarılıysa True, hata oluştuysa False döner.
    """
    try:
        if not skins_list:
            return True

        now_str = datetime.utcnow().isoformat()
        formatted_list = []
        seen_in_batch = set()

        for skin in skins_list:
            s_id = skin.get("skin_id")
            if not s_id or s_id in _known_skin_ids_cache or s_id in seen_in_batch:
                continue
            seen_in_batch.add(s_id)

            formatted_skin = {
                "skin_id": s_id,
                "skin_name": skin.get("skin_name", ""),
                "wear_value": skin.get("wear_value", ""),
                "processed_at": skin.get("processed_at", now_str)
            }
            formatted_list.append(formatted_skin)

        if not formatted_list:
            return True

        # Upsert + ignore_duplicates sayesinde 1 kayıt bile zaten varsa hata vermez, kalanları yazar
        _execute_with_retry(
            lambda client: client.table("skins").upsert(
                formatted_list,
                on_conflict="skin_id",
                ignore_duplicates=True
            ).execute(),
            action_name="Bulk Upsert (skins)",
            retries=2,
        )

        for item in formatted_list:
            _known_skin_ids_cache.add(item["skin_id"])

        logger.info(f"{len(formatted_list)} adet yeni skin toplu olarak (Bulk Upsert) buluta eklendi.")
        return True
    except Exception as e:
        logger.error(f"Toplu skin kaydetme hatası (Bulk Upsert): {e}")
        return False


async def save_skin_async(skin: dict):
    async with db_semaphore:
        await asyncio.to_thread(save_skins_batch, [skin])


def delete_skin(skin_id: str):
    _known_skin_ids_cache.discard(skin_id)


def delete_skins_batch(skin_ids: list):
    for s_id in skin_ids:
        _known_skin_ids_cache.discard(s_id)


def clear_all_skins():
    global _known_skin_ids_cache, _skin_ids_cache_loaded, _last_scanned_id_cache
    _known_skin_ids_cache.clear()
    _skin_ids_cache_loaded = True
    _last_scanned_id_cache = ""

    try:
        _execute_with_retry(
            lambda client: client.table("skins").delete().neq("id", -1).execute(),
            action_name="clear_all_skins",
            retries=1,
        )
        logger.info("Veritabanı temizlendi (Tüm skin kayıtları silindi).")
    except Exception as e:
        logger.error(f"Veritabanı temizleme hatası: {e}")

    try:
        _execute_with_retry(
            lambda client: client.table("system_state")
            .delete()
            .in_("key_name", ["last_scanned_id", "last_scanned_name"])
            .execute(),
            action_name="clear_system_state",
            retries=1,
        )
        logger.info("Son taranan ürün hafızası temizlendi.")
    except Exception:
        pass


def get_last_scanned_id() -> str:
    """Önce RAM'e bakar. Sadece başlangıçta 1 kere Supabase'e sorar."""
    global _last_scanned_id_cache
    if _last_scanned_id_cache is not None:
        return _last_scanned_id_cache

    try:
        response = _execute_with_retry(
            lambda client: client.table("system_state")
            .select("key_value")
            .eq("key_name", "last_scanned_id")
            .limit(1)
            .execute(),
            action_name="get_last_scanned_id",
            retries=1,
        )
        if response.data:
            _last_scanned_id_cache = response.data[0]["key_value"]
        else:
            _last_scanned_id_cache = ""
        return _last_scanned_id_cache
    except Exception:
        _last_scanned_id_cache = ""
        return ""


def set_last_scanned_id(skin_id: str, skin_name: str = ""):
    """
    Son okunan eşyayı anında RAM'de günceller.
    Veritabanı IO ve ağ tıkanıklığını önlemek için Supabase'e en fazla 60 saniyede bir yazar.
    """
    global _last_scanned_id_cache, _last_state_sync_time
    if not skin_id or skin_id == _last_scanned_id_cache:
        return

    _last_scanned_id_cache = skin_id
    logger.info(f"Son okunan eşya güncellendi: {skin_name} (ID: {skin_id})")

    now_ts = time.time()
    if now_ts - _last_state_sync_time < 60.0:
        return

    _last_state_sync_time = now_ts
    try:
        now_str = datetime.utcnow().isoformat()
        updates = [
            {"key_name": "last_scanned_id", "key_value": skin_id, "updated_at": now_str}
        ]
        if skin_name:
            updates.append({"key_name": "last_scanned_name", "key_value": skin_name, "updated_at": now_str})

        _execute_with_retry(
            lambda client: client.table("system_state").upsert(updates, on_conflict="key_name").execute(),
            action_name="set_last_scanned_id",
            retries=1,
        )
    except Exception as e:
        logger.debug(f"Son taranan eşya DB senkronizasyon uyarısı (RAM üzerinden devam ediliyor): {e}")


def get_all_skin_ids() -> set:
    """Her döngüde tüm tabloyu okumak yerine RAM'deki kümeden yanıt verir."""
    global _known_skin_ids_cache, _skin_ids_cache_loaded
    if _skin_ids_cache_loaded:
        # Bellek şişmesini önlemek için 5000'i geçerse en eski yarısını temizle
        if len(_known_skin_ids_cache) > 5000:
            _known_skin_ids_cache.clear()
        return _known_skin_ids_cache.copy()

    try:
        response = _execute_with_retry(
            lambda client: client.table("skins").select("skin_id").execute(),
            action_name="get_all_skin_ids",
            retries=1,
        )
        _known_skin_ids_cache = {row["skin_id"] for row in response.data}
        _skin_ids_cache_loaded = True
        return _known_skin_ids_cache.copy()
    except Exception as e:
        logger.error(f"Skin ID'lerini çekme hatası: {e}")
        return _known_skin_ids_cache.copy()


def get_work_batch(limit: int) -> list:
    """
    Supabase'den en fazla 'limit' kadar ürünü alır ve atomik olarak siler.
    """
    try:
        response = _execute_with_retry(
            lambda client: client.rpc("get_and_delete_skins", {"batch_limit": limit}).execute(),
            action_name="get_work_batch (RPC)",
            retries=2,
        )
        data = response.data or []
        with _db_lock:
            for row in data:
                _known_skin_ids_cache.discard(row.get("skin_id"))
        return data
    except Exception as e:
        logger.error(f"Supabase toplu iş çekme hatası: {e}")
        return []


def is_bargain_notified(skin_name: str, seller: str, float_value: str = "") -> bool:
    key = (skin_name, seller, float_value)
    if key in _notified_cache:
        return True

    try:
        response = _execute_with_retry(
            lambda client: (
                client.table("notified_bargains")
                .select("id")
                .eq("skin_name", skin_name)
                .eq("seller", seller)
                .eq("float_value", float_value)
                .limit(1)
                .execute()
            ),
            action_name="is_bargain_notified",
            retries=1,
        )
        exists = len(response.data) > 0
        if exists:
            _notified_cache.add(key)
        return exists
    except Exception as e:
        logger.error(f"Bargain bildirim kontrolü hatası: {e}")
        return False


def mark_bargain_notified(skin_name: str, seller: str, float_value: str = ""):
    key = (skin_name, seller, float_value)
    if key in _notified_cache:
        return
    _notified_cache.add(key)

    try:
        _execute_with_retry(
            lambda client: client.table("notified_bargains").insert({
                "skin_name": skin_name,
                "seller": seller,
                "float_value": float_value,
                "notified_at": datetime.utcnow().isoformat()
            }).execute(),
            action_name="mark_bargain_notified",
            retries=1,
        )
        logger.debug(f"Bargain DB'ye kaydedildi: {skin_name} - Satıcı: {seller} - Float: {float_value}")
    except Exception as e:
        if "duplicate key value" not in str(e).lower() and "violates unique constraint" not in str(e).lower():
            logger.error(f"Bargain kaydetme hatası: {e}")


def log_skipped_bargain(skin_name: str, seller: str, float_value: str = ""):
    """
    DİSK IO KORUMASI: Atlanan her ürünü veritabanına INSERT etmek Disk IO bütçesini
    tükettiği için artık sadece konsola/log dosyasına yazıyoruz.
    """
    logger.debug(f"[Atlandı - DB Yazılmadı] {skin_name} - Satıcı: {seller} - Float: {float_value}")
