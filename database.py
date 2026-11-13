# -*- coding: utf-8 -*-
"""
Cloud Task Sync — Veritabanı Modülü (Supabase)
Optimize Edilmiş Versiyon: Kilitsiz (Non-Blocking) Thread İstekleri, RAM Cache,
Otomatik 5000/4000 Temizliği (Trim), Upsert Ignore Duplicates, Returning Minimal ve Kademeli Backoff.
"""

import asyncio
from collections import OrderedDict
from datetime import datetime
import logging
import threading
import time

from config import (
    KNOWN_SKIN_CACHE_MAX,
    NOTIFIED_CACHE_MAX,
    SUPABASE_KEY,
    SUPABASE_URL,
)
import httpx
from supabase import Client, ClientOptions, create_client

# httpx seviyesinde HTTP/2'yi kapat ve hiçbir işlemin 10 saniyeden fazla asılı kalmamasını garantile
_original_client_init = httpx.Client.__init__


def _patched_client_init(self, *args, **kwargs):
    kwargs["http2"] = False
    kwargs["timeout"] = httpx.Timeout(
        10.0, connect=5.0, read=10.0, write=10.0, pool=5.0
    )
    kwargs["limits"] = httpx.Limits(
        max_connections=5, max_keepalive_connections=3, keepalive_expiry=65.0
    )
    _original_client_init(self, *args, **kwargs)


httpx.Client.__init__ = _patched_client_init

logger = logging.getLogger(__name__)

supabase: Client = None
_client_lock = threading.Lock()
_cache_lock = threading.RLock()

# Eşzamanlı (Concurrent) İstekleri Sınırlandırmak için Semaphore (Supabase RAM Koruması)
db_semaphore = asyncio.Semaphore(1)

# ==========================================================
# DİSK IO OPTİMİZASYONU: BELLEK İÇİ ÖNBELLEKLER (RAM CACHE - FIFO)
# ==========================================================
_notified_cache: OrderedDict = OrderedDict()  # (skin_name, seller, float_value) -> None
_known_skin_ids_cache: OrderedDict = OrderedDict()  # skin_id -> None
_skin_ids_cache_loaded = False
_last_scanned_id_cache = None  # Son taranan skin_id
_last_scanned_name_cache = ""  # Son taranan skin_name
_last_synced_scanned_id = None  # Supabase'e en son yazılan skin_id
_last_state_sync_time = (
    0.0  # system_state tablosuna son yazma zamanı (Throttling)
)
_skin_insert_buffer = []  # Tekli save_skin çağrıları için tampon


def _cache_put_fifo(
    cache: OrderedDict, key, max_size: int = KNOWN_SKIN_CACHE_MAX
):
    """Önbelleğe FIFO sırayla ekler; kapasite aşımında en eski kaydı çıkarır."""
    if key in cache:
        cache.move_to_end(key)
        return
    cache[key] = None
    while len(cache) > max_size:
        cache.popitem(last=False)


def add_known_skin_ids(skin_ids) -> None:
    """Verilen skin_id koleksiyonunu 3000 kapasiteli FIFO OrderedDict önbelleğine ekler."""
    with _cache_lock:
        for s_id in skin_ids:
            if s_id:
                _cache_put_fifo(
                    _known_skin_ids_cache, s_id, KNOWN_SKIN_CACHE_MAX
                )


def _get_or_create_client() -> Client:
    """Thread-safe olarak mevcut Supabase istemcisini döndürür.

    Hata anında istemciyi sıfırdan oluşturmaz; mevcut TCP/SSL havuzunu korur.
    """
    global supabase
    if supabase is not None:
        return supabase

    with _client_lock:
        if supabase is not None:
            return supabase

        options = ClientOptions(postgrest_client_timeout=10)
        client = create_client(SUPABASE_URL, SUPABASE_KEY, options=options)

        if hasattr(client, "postgrest") and hasattr(client.postgrest, "client"):
            try:
                client.postgrest.client._http2 = False
            except Exception:
                pass

        supabase = client
        return supabase


def _is_recoverable_network_error(err: Exception) -> bool:
    """Timeout, statement timeout, kopan soket veya geçici ağ hatalarını tespit eder."""
    msg = str(err).lower()
    keywords = (
        "timed out",
        "timeout",
        "57014",
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


def _execute_with_retry(
    operation_fn, action_name: str = "DB İşlemi", retries: int = 2
):
    """Supabase sorgularını global kilit tutmadan (non-blocking) çalıştırır.

    Hata alındığında yeni bağlantı açıp sunucuyu boğmak yerine mevcut istemciyle
    ilk hatada 10 saniye, ikinci hatada 25 saniye bekleyerek (backoff) tekrar
    dener.
    """
    backoff_durations = (10.0, 25.0)
    last_err = None
    client = _get_or_create_client()

    for attempt in range(retries + 1):
        try:
            return operation_fn(client)
        except Exception as e:
            last_err = e
            if _is_recoverable_network_error(e) and attempt < retries:
                backoff = backoff_durations[
                    min(attempt, len(backoff_durations) - 1)
                ]
                logger.warning(
                    f"[DB Retry] '{action_name}' sırasında ağ/zaman aşımı"
                    f" ({e}). {backoff:.0f} sn beklenip mevcut bağlantıyla"
                    f" tekrar denenecek (Deneme {attempt + 1}/{retries})..."
                )
                time.sleep(backoff)
            else:
                break
    raise last_err


def init_db():
    try:
        _get_or_create_client()
        logger.info("Supabase bağlantısı başarıyla başlatıldı.")
        _preload_notified_cache()
        last_id = get_last_scanned_id()
        logger.info(
            f"[Kalıcı Hafıza] Başlangıç last_scanned_id: {last_id or '(yok)'}"
        )
    except Exception as e:
        logger.error(f"Supabase başlatma hatası: {e}")
        raise


def _preload_notified_cache():
    """Başlangıçta son 3000 bildirimi sayfalı (1000'erlik) olarak FIFO RAM önbelleğine çeker."""
    try:
        all_rows = []
        page_size = 1000
        for offset in range(0, NOTIFIED_CACHE_MAX, page_size):
            end_idx = min(offset + page_size - 1, NOTIFIED_CACHE_MAX - 1)
            res = _execute_with_retry(
                lambda client, start=offset, end=end_idx: client.table(
                    "notified_bargains"
                )
                .select("skin_name, seller, float_value")
                .order("id", desc=True)
                .range(start, end)
                .execute(),
                action_name=f"Preload Notified Cache ({offset}-{end_idx})",
                retries=1,
            )
            batch = res.data or []
            all_rows.extend(batch)
            if len(batch) < (end_idx - offset + 1):
                break

        if all_rows:
            with _cache_lock:
                for row in reversed(all_rows):
                    key = (
                        row.get("skin_name", ""),
                        row.get("seller", ""),
                        row.get("float_value", ""),
                    )
                    _cache_put_fifo(_notified_cache, key, NOTIFIED_CACHE_MAX)
            logger.info(
                f"[RAM Cache] {len(_notified_cache)} adet geçmiş bildirim"
                " önbelleğe yüklendi."
            )
    except Exception as e:
        logger.debug(f"Önbellek yükleme uyarısı: {e}")


def is_skin_exists(skin_id: str) -> bool:
    with _cache_lock:
        if skin_id in _known_skin_ids_cache:
            return True
    try:
        response = _execute_with_retry(
            lambda client: client.table("skins")
            .select("id")
            .eq("skin_id", skin_id)
            .limit(1)
            .execute(),
            action_name="is_skin_exists",
            retries=1,
        )
        exists = len(response.data) > 0
        if exists:
            with _cache_lock:
                _cache_put_fifo(
                    _known_skin_ids_cache, skin_id, KNOWN_SKIN_CACHE_MAX
                )
        return exists
    except Exception as e:
        logger.error(f"Skin kontrolü hatası (skin_id={skin_id}): {e}")
        return False


def save_skin(skin_id: str, skin_name: str = "", wear_value: str = ""):
    """Tekli çağrıları tamponda biriktirip 50'şerli toplu yazar."""
    with _cache_lock:
        if skin_id in _known_skin_ids_cache:
            return
        _skin_insert_buffer.append(
            {
                "skin_id": skin_id,
                "skin_name": skin_name,
                "wear_value": wear_value,
            }
        )
        should_flush = len(_skin_insert_buffer) >= 50
    if should_flush:
        flush_skin_buffer()


def flush_skin_buffer():
    global _skin_insert_buffer
    with _cache_lock:
        if not _skin_insert_buffer:
            return
        to_save = _skin_insert_buffer[:]
        _skin_insert_buffer.clear()
    save_skins_batch(to_save)


def save_skins_batch(skins_list: list) -> bool:
    """Toplu Ekleme (Bulk Upsert) - Çakışan (Duplicate) kayıtları sessizce yoksayar (ignore_duplicates=True)

    ve eklenen satırları geri döndürmeyerek (returning="minimal") Disk I/O
    tüketimini minimuma indirir.
    """
    try:
        if not skins_list:
            return True

        now_str = datetime.utcnow().isoformat()
        formatted_list = []
        seen_in_batch = set()

        with _cache_lock:
            for skin in skins_list:
                s_id = skin.get("skin_id")
                if (
                    not s_id
                    or s_id in _known_skin_ids_cache
                    or s_id in seen_in_batch
                ):
                    continue
                seen_in_batch.add(s_id)

                formatted_skin = {
                    "skin_id": s_id,
                    "skin_name": skin.get("skin_name", ""),
                    "wear_value": skin.get("wear_value", ""),
                    "processed_at": skin.get("processed_at", now_str),
                }
                formatted_list.append(formatted_skin)

        if not formatted_list:
            return True

        # En güncel verileri önceliklendir: Tek döngüde 3 Worker'ın taze eritebileceğinden fazlasını (max 20) DB'ye yığma
        with _cache_lock:
            for item in formatted_list:
                _cache_put_fifo(
                    _known_skin_ids_cache, item["skin_id"], KNOWN_SKIN_CACHE_MAX
                )

        formatted_list = formatted_list[:20]

        # Tek seferde upsert yaparak PostgREST istek yükünü azalt
        chunk_size = 50
        for i in range(0, len(formatted_list), chunk_size):
            chunk = formatted_list[i : i + chunk_size]
            _execute_with_retry(
                lambda client, ch=chunk: client.table("skins")
                .upsert(
                    ch,
                    on_conflict="skin_id",
                    ignore_duplicates=True,  # Zaten varsa diske tekrar yazma (ON CONFLICT DO NOTHING)
                    returning="minimal",  # Eklenen satırları geri döndürme, Disk I/O ve ağ tasarrufu yap
                )
                .execute(),
                action_name=f"Bulk Upsert (skins {i+1}-{i+len(chunk)})",
                retries=2,
            )

        logger.info(
            f"{len(formatted_list)} adet yeni skin toplu olarak (Bulk Upsert)"
            " buluta eklendi."
        )
        return True
    except Exception as e:
        logger.error(f"Toplu skin kaydetme hatası (Bulk Upsert): {e}")
        return False


def _trim_table_by_pk(
    table_name: str, max_rows: int = 150, delete_count: int = 100
) -> int:
    """Primary Key (id) indeksini kullanarak COUNT(*) tablosu taraması yapmadan

    tablo boyutu max_rows'u (150) aşmışsa en eski delete_count (100) kaydı
    siler.
    """
    try:
        client = _get_or_create_client()
        check_res = (
            client.table(table_name)
            .select("id")
            .order("id", desc=False)
            .range(max_rows, max_rows)
            .execute()
        )
        if not check_res.data:
            return 0

        cutoff_res = (
            client.table(table_name)
            .select("id")
            .order("id", desc=False)
            .range(delete_count, delete_count)
            .execute()
        )
        if not cutoff_res.data:
            return 0

        cutoff_id = cutoff_res.data[0]["id"]
        client.table(table_name).delete(returning="minimal").lt(
            "id", cutoff_id
        ).execute()
        return delete_count
    except Exception as e:
        logger.debug(f"[{table_name}] PK tabanlı temizlik uyarısı: {e}")
        return 0


def trim_old_skins(max_rows: int = 150, delete_count: int = 100) -> int:
    """Veritabanındaki kayıt sayısı max_rows'u (150) geçerse en eski delete_count (100) taneyi siler.

    Böylece Supabase RAM/Swap şişmez ve Worker'lar her zaman en güncel ürünleri çeker.
    """
    total_deleted = 0
    try:
        client = _get_or_create_client()
        res = client.rpc(
            "trim_old_skins",
            {"max_rows": max_rows, "delete_count": delete_count},
        ).execute()
        if isinstance(res.data, int):
            total_deleted = res.data
    except Exception:
        deleted_s = _trim_table_by_pk(
            "skins", max_rows=max_rows, delete_count=delete_count
        )
        deleted_n = _trim_table_by_pk(
            "notified_bargains", max_rows=NOTIFIED_CACHE_MAX, delete_count=200
        )
        total_deleted = deleted_s + deleted_n

    if total_deleted > 0:
        logger.info(
            f"🧹 Veritabanı temizliği: En eski {total_deleted} kayıt silindi"
            f" ({max_rows} taze kuyruk sınırı korundu)."
        )
    return total_deleted


async def save_skin_async(skin: dict):
    async with db_semaphore:
        await asyncio.to_thread(save_skins_batch, [skin])


def delete_skin(skin_id: str):
    with _cache_lock:
        _known_skin_ids_cache.pop(skin_id, None)


def delete_skins_batch(skin_ids: list):
    with _cache_lock:
        for s_id in skin_ids:
            _known_skin_ids_cache.pop(s_id, None)


def clear_all_skins():
    global _known_skin_ids_cache, _skin_ids_cache_loaded, _last_scanned_id_cache
    with _cache_lock:
        _known_skin_ids_cache.clear()
        _skin_ids_cache_loaded = True
        _last_scanned_id_cache = ""

    try:
        _execute_with_retry(
            lambda client: client.table("skins")
            .delete(returning="minimal")
            .neq("id", -1)
            .execute(),
            action_name="clear_all_skins",
            retries=1,
        )
        logger.info("Veritabanı temizlendi (Tüm skin kayıtları silindi).")
    except Exception as e:
        logger.error(f"Veritabanı temizleme hatası: {e}")

    try:
        _execute_with_retry(
            lambda client: client.table("system_state")
            .delete(returning="minimal")
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
    global _last_scanned_id_cache, _last_synced_scanned_id
    with _cache_lock:
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
        val = response.data[0]["key_value"] if response.data else ""
        with _cache_lock:
            _last_scanned_id_cache = val
            _last_synced_scanned_id = val
        return val
    except Exception:
        with _cache_lock:
            _last_scanned_id_cache = ""
        return ""


def set_last_scanned_id(skin_id: str, skin_name: str = "", force: bool = False):
    """Son okunan eşyayı anında RAM'de günceller.

    Veritabanı IO ve ağ tıkanıklığını önlemek için Supabase'e en fazla 180
    saniyede bir yazar (force=True ile kapanışta anında yazılabilir).
    """
    global _last_scanned_id_cache, _last_scanned_name_cache, _last_synced_scanned_id, _last_state_sync_time
    with _cache_lock:
        if not skin_id:
            return
        if skin_id != _last_scanned_id_cache:
            _last_scanned_id_cache = skin_id
            if skin_name:
                _last_scanned_name_cache = skin_name
            logger.info(
                f"Son okunan eşya güncellendi: {skin_name} (ID: {skin_id})"
            )
        elif not force or skin_id == _last_synced_scanned_id:
            return

        now_ts = time.time()
        if not force and now_ts - _last_state_sync_time < 180.0:
            return
        _last_state_sync_time = now_ts
        name_to_sync = skin_name or _last_scanned_name_cache

    try:
        now_str = datetime.utcnow().isoformat()
        updates = [
            {
                "key_name": "last_scanned_id",
                "key_value": skin_id,
                "updated_at": now_str,
            }
        ]
        if name_to_sync:
            updates.append(
                {
                    "key_name": "last_scanned_name",
                    "key_value": name_to_sync,
                    "updated_at": now_str,
                }
            )

        _execute_with_retry(
            lambda client: client.table("system_state")
            .upsert(
                updates,
                on_conflict="key_name",
                returning="minimal",
            )
            .execute(),
            action_name="set_last_scanned_id",
            retries=1,
        )
        with _cache_lock:
            _last_synced_scanned_id = skin_id
    except Exception as e:
        logger.debug(
            "Son taranan eşya DB senkronizasyon uyarısı (RAM üzerinden devam"
            f" ediliyor): {e}"
        )


def flush_last_scanned_id():
    """Kapanış sırasında RAM'deki son taranan ID'yi Supabase'e zorla senkronize eder."""
    with _cache_lock:
        skin_id = _last_scanned_id_cache
        skin_name = _last_scanned_name_cache
        synced = _last_synced_scanned_id
    if skin_id and skin_id != synced:
        set_last_scanned_id(skin_id, skin_name, force=True)


def set_ws_url(ws_url: str) -> bool:
    """Ana botun aktif wss:// tünel adresini system_state tablosuna yazar."""
    try:
        now_str = datetime.utcnow().isoformat()
        _execute_with_retry(
            lambda client: client.table("system_state")
            .upsert(
                {
                    "key_name": "ws_url",
                    "key_value": ws_url or "",
                    "updated_at": now_str,
                },
                on_conflict="key_name",
                returning="minimal",
            )
            .execute(),
            action_name="set_ws_url",
            retries=2,
        )
        logger.info(
            f"[System State] ws_url güncellendi: {ws_url or '(temizlendi)'}"
        )
        return True
    except Exception as e:
        logger.error(f"ws_url kaydetme hatası: {e}")
        return False


def get_ws_url() -> str:
    """Worker botlar için system_state tablosundan güncel wss:// tünel adresini okur."""
    try:
        response = _execute_with_retry(
            lambda client: client.table("system_state")
            .select("key_value")
            .eq("key_name", "ws_url")
            .limit(1)
            .execute(),
            action_name="get_ws_url",
            retries=1,
        )
        if response.data:
            return (response.data[0].get("key_value") or "").strip()
        return ""
    except Exception as e:
        logger.debug(f"ws_url okuma uyarısı: {e}")
        return ""


def get_all_skin_ids() -> set:
    """Her döngüde tüm tabloyu okumak yerine RAM'deki FIFO önbellekten yanıt verir."""
    global _known_skin_ids_cache, _skin_ids_cache_loaded
    with _cache_lock:
        if _skin_ids_cache_loaded:
            return set(_known_skin_ids_cache.keys())

    try:
        response = _execute_with_retry(
            lambda client: client.table("skins")
            .select("skin_id")
            .order("id", desc=True)
            .limit(150)
            .execute(),
            action_name="get_all_skin_ids",
            retries=1,
        )
        with _cache_lock:
            _known_skin_ids_cache.clear()
            for row in reversed(response.data or []):
                s_id = row.get("skin_id")
                if s_id:
                    _cache_put_fifo(
                        _known_skin_ids_cache, s_id, KNOWN_SKIN_CACHE_MAX
                    )
            _skin_ids_cache_loaded = True
            return set(_known_skin_ids_cache.keys())
    except Exception as e:
        logger.error(f"Skin ID'lerini çekme hatası: {e}")
        with _cache_lock:
            return set(_known_skin_ids_cache.keys())


def get_work_batch(limit: int) -> list:
    """Supabase'den en fazla 'limit' kadar ürünü alır ve atomik olarak siler."""
    try:
        response = _execute_with_retry(
            lambda client: client.rpc(
                "get_and_delete_skins", {"batch_limit": limit}
            ).execute(),
            action_name="get_work_batch (RPC)",
            retries=2,
        )
        data = response.data or []
        return data
    except Exception as e:
        logger.error(f"Supabase toplu iş çekme hatası: {e}")
        return []


def is_bargain_notified(
    skin_name: str, seller: str, float_value: str = ""
) -> bool:
    """Ekstra SELECT sorgusu atmadan doğrudan önbellekten kontrol eder (Disk IO & RAM Koruması)."""
    key = (skin_name, seller, float_value)
    with _cache_lock:
        return key in _notified_cache


def mark_bargain_notified(skin_name: str, seller: str, float_value: str = ""):
    key = (skin_name, seller, float_value)
    with _cache_lock:
        if key in _notified_cache:
            return
        _cache_put_fifo(_notified_cache, key, NOTIFIED_CACHE_MAX)

    try:
        _execute_with_retry(
            lambda client: client.table("notified_bargains")
            .upsert(
                {
                    "skin_name": skin_name,
                    "seller": seller,
                    "float_value": float_value,
                    "notified_at": datetime.utcnow().isoformat(),
                },
                on_conflict="skin_name,seller,float_value",
                ignore_duplicates=True,
                returning="minimal",  # Eklenen satırı geri döndürmeyerek Disk I/O tasarrufu yapar
            )
            .execute(),
            action_name="mark_bargain_notified",
            retries=1,
        )
        logger.debug(
            f"Bargain DB'ye kaydedildi: {skin_name} - Satıcı: {seller} - Float:"
            f" {float_value}"
        )
    except Exception as e:
        if (
            "duplicate key value" not in str(e).lower()
            and "violates unique constraint" not in str(e).lower()
        ):
            logger.error(f"Bargain kaydetme hatası: {e}")


def log_skipped_bargain(skin_name: str, seller: str, float_value: str = ""):
    """DİSK IO KORUMASI: Atlanan her ürünü veritabanına INSERT etmek Disk IO bütçesini

    tükettiği için sadece konsola/log dosyasına yazıyoruz.
    """
    logger.debug(
        f"[Atlandı - DB Yazılmadı] {skin_name} - Satıcı: {seller} - Float:"
        f" {float_value}"
    )
