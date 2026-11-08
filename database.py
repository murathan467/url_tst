# -*- coding: utf-8 -*-
"""
Cloud Task Sync — Veritabanı Modülü (Supabase)
Supabase bulut veritabanı işlemleri: kayıt, kontrol, toplu iş çekme.
"""

import logging
from datetime import datetime
import asyncio
from supabase import create_client, Client, ClientOptions

from config import SUPABASE_URL, SUPABASE_KEY
import httpx

# Çözüm 3: httpx seviyesinde HTTP/2'yi zorla devre dışı bırakıyoruz (Monkey Patch)
_original_client_init = httpx.Client.__init__
def _patched_client_init(self, *args, **kwargs):
    if 'http2' in kwargs:
        kwargs['http2'] = False
    _original_client_init(self, *args, **kwargs)
httpx.Client.__init__ = _patched_client_init

logger = logging.getLogger(__name__)

supabase: Client = None

# Çözüm 2 için Semaphore: Aynı anda en fazla 5 veritabanı isteğine izin ver
db_semaphore = asyncio.Semaphore(5)

def init_db():
    global supabase
    try:
        # Çözüm 3: Supabase İstemcisinde HTTP/2'yi Kapatmak (Geçici Alternatif)
        options = ClientOptions(postgrest_client_timeout=60)
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY, options=options)
        
        # Httpx istemcisinde http2'yi kapatmak
        if hasattr(supabase, 'postgrest') and hasattr(supabase.postgrest, 'client'):
            try:
                supabase.postgrest.client._http2 = False
            except Exception:
                pass
                
        logger.info("Supabase bağlantısı başarıyla başlatıldı.")
    except Exception as e:
        logger.error(f"Supabase başlatma hatası: {e}")
        raise

def is_skin_exists(skin_id: str) -> bool:
    try:
        if not supabase: init_db()
        response = supabase.table("skins").select("id").eq("skin_id", skin_id).limit(1).execute()
        return len(response.data) > 0
    except Exception as e:
        logger.error(f"Skin kontrolü hatası (skin_id={skin_id}): {e}")
        return False

def save_skin(skin_id: str, skin_name: str = "", wear_value: str = ""):
    try:
        if not supabase: init_db()
        supabase.table("skins").insert({
            "skin_id": skin_id,
            "skin_name": skin_name,
            "wear_value": wear_value,
            "processed_at": datetime.utcnow().isoformat()
        }).execute()
        wear_info = f" ({wear_value})" if wear_value else ""
        logger.info(f"Skin buluta eklendi: {skin_name}{wear_info} (ID: {skin_id})")
    except Exception as e:
        # Tekrarlayan anahtar hatalarını (duplicate key) yoksay
        if "duplicate key value" not in str(e).lower() and "violates unique constraint" not in str(e).lower():
            logger.error(f"Skin kaydetme hatası (skin_id={skin_id}): {e}")

def save_skins_batch(skins_list: list):
    """
    Çözüm 1: Toplu Ekleme (Bulk Insert) - En Kesin Çözüm
    Veritabanına veri yazarken bunları tek tek değil, bir liste halinde tek seferde gönderir.
    """
    try:
        if not skins_list:
            return
        if not supabase: init_db()
        
        now_str = datetime.utcnow().isoformat()
        formatted_list = []
        for skin in skins_list:
            formatted_skin = skin.copy()
            if "processed_at" not in formatted_skin:
                formatted_skin["processed_at"] = now_str
            formatted_list.append(formatted_skin)
            
        response = supabase.table("skins").insert(formatted_list).execute()
        logger.info(f"{len(formatted_list)} adet skin toplu olarak (Bulk Insert) eklendi.")
        return response
    except Exception as e:
        logger.error(f"Toplu skin kaydetme hatası (Bulk Insert): {e}")

async def save_skin_async(skin: dict):
    """
    Çözüm 2: Eşzamanlı (Concurrent) İstekleri Sınırlandırmak
    Aynı anda en fazla 5 veritabanı isteğine izin ver
    """
    async with db_semaphore:
        try:
            if not supabase: init_db()
            now_str = datetime.utcnow().isoformat()
            skin_copy = skin.copy()
            if "processed_at" not in skin_copy:
                skin_copy["processed_at"] = now_str
            response = supabase.table("skins").insert(skin_copy).execute()
            logger.info(f"Skin async olarak eklendi: {skin_copy.get('skin_id')}")
            return response
        except Exception as e:
            logger.error(f"Async skin kaydetme hatası: {e}")

def delete_skin(skin_id: str):
    """
    Kuyrukta olan ürünler get_and_delete_skins SQL fonksiyonu ile Supabase
    tarafından çekildiği anda otomatik olarak silinir.
    O yüzden artık DB'den işlem sonunda bu fonksiyona iş düşmüyor.
    """
    pass

def delete_skins_batch(skin_ids: list):
    """
    Artık kullanılmıyor. Otomatik silme işlemi RPC ile sağlanıyor.
    """
    pass

def clear_all_skins():
    try:
        if not supabase: init_db()
        supabase.table("skins").delete().neq("id", -1).execute()
        logger.info("Veritabanı temizlendi (Tüm skin kayıtları silindi).")
    except Exception as e:
        logger.error(f"Veritabanı temizleme hatası: {e}")
        
    try:
        if not supabase: init_db()
        supabase.table("system_state").delete().in_("key_name", ["last_scanned_id", "last_scanned_name"]).execute()
        logger.info("Son taranan ürün hafızası temizlendi.")
    except Exception as e:
        pass

def get_last_scanned_id() -> str:
    try:
        if not supabase: init_db()
        response = supabase.table("system_state").select("key_value").eq("key_name", "last_scanned_id").limit(1).execute()
        if response.data:
            return response.data[0]["key_value"]
        return ""
    except Exception as e:
        return ""

def set_last_scanned_id(skin_id: str, skin_name: str = ""):
    try:
        if not supabase: init_db()
        now_str = datetime.utcnow().isoformat()
        
        updates = []
        updates.append({"key_name": "last_scanned_id", "key_value": skin_id, "updated_at": now_str})
        if skin_name:
            updates.append({"key_name": "last_scanned_name", "key_value": skin_name, "updated_at": now_str})
            
        # Tek seferde upsert atıyoruz (tabloda key_name unique ise çalışır, daha hızlıdır)
        # Eğer unique constraint yoksa mevcut kod gibi ayrı ayrı da atılabilir.
        try:
            supabase.table("system_state").upsert(updates, on_conflict="key_name").execute()
            logger.info(f"Son okunan eşya güncellendi: {skin_name} (ID: {skin_id})")
        except Exception:
            # Fallback (Eğer on_conflict desteklenmiyorsa veya constraint ismi farklıysa)
            for item in updates:
                res = supabase.table("system_state").select("id").eq("key_name", item["key_name"]).execute()
                if len(res.data) > 0:
                    supabase.table("system_state").update(item).eq("key_name", item["key_name"]).execute()
                else:
                    supabase.table("system_state").insert(item).execute()
            logger.info(f"Son okunan eşya güncellendi: {skin_name} (ID: {skin_id})")
            
    except Exception as e:
        logger.error(f"Son taranan eşya güncelleme hatası: {e}")

def get_all_skin_ids() -> set:
    try:
        if not supabase: init_db()
        # Supabase'den çekebildiğin kadar skinID al (1000 satır sınırı olabilir, o yüzden sadece bekleyenleri tuttuğumuz için sorun yok)
        response = supabase.table("skins").select("skin_id").execute()
        ids = {row["skin_id"] for row in response.data}
        return ids
    except Exception as e:
        logger.error(f"Skin ID'lerini çekme hatası: {e}")
        return set()

def get_work_batch(limit: int) -> list:
    """
    Supabase'den en fazla 'limit' kadar ürünü alır ve atomik işlem olarak çeker çekmez veritabanından siler.
    Böylece yerel bilgisayar ile VM'in aynı ürünü işleme şansı kalmaz.
    """
    try:
        if not supabase: init_db()
        response = supabase.rpc("get_and_delete_skins", {"batch_limit": limit}).execute()
        # Supabase, RPC sonucunu data içerisinde dict dizisi olarak döner
        return response.data
    except Exception as e:
        logger.error(f"Supabase toplu iş çekme hatası: {e}")
        return []

def is_bargain_notified(skin_name: str, seller: str, float_value: str = "") -> bool:
    try:
        if not supabase: init_db()
        response = supabase.table("notified_bargains").select("id").eq("skin_name", skin_name).eq("seller", seller).eq("float_value", float_value).limit(1).execute()
        return len(response.data) > 0
    except Exception as e:
        logger.error(f"Bargain bildirim kontrolü hatası: {e}")
        return False

def mark_bargain_notified(skin_name: str, seller: str, float_value: str = ""):
    try:
        if not supabase: init_db()
        supabase.table("notified_bargains").insert({
            "skin_name": skin_name,
            "seller": seller,
            "float_value": float_value,
            "notified_at": datetime.utcnow().isoformat()
        }).execute()
        logger.debug(f"Bargain DB'ye kaydedildi (Tekrar bildirim gitmeyecek): {skin_name} - Satıcı: {seller} - Float: {float_value}")
    except Exception as e:
        if "duplicate key value" not in str(e).lower() and "violates unique constraint" not in str(e).lower():
            logger.error(f"Bargain kaydetme hatası: {e}")

def log_skipped_bargain(skin_name: str, seller: str, float_value: str = ""):
    """Aynı floatta olan ürünler bildirilmeden atlandığında yeni DB tablosuna ekler."""
    try:
        if not supabase: init_db()
        supabase.table("skipped_bargains").insert({
            "skin_name": skin_name,
            "seller": seller,
            "float_value": float_value,
            "skipped_at": datetime.utcnow().isoformat()
        }).execute()
        logger.debug(f"Atlanan skin DB'ye kaydedildi: {skin_name} - Satıcı: {seller} - Float: {float_value}")
    except Exception as e:
        if "duplicate key value" not in str(e).lower() and "violates unique constraint" not in str(e).lower():
            logger.error(f"Atlanan skin kaydetme hatası: {e}")
