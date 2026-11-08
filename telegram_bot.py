# -*- coding: utf-8 -*-
"""
Cloud Task Sync — Bildirim Modülü
Telegram Bot API üzerinden mesaj gönderimi.
"""

import logging
import requests

from config import TELEGRAM_API_URL, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)


def send_notification(skin_name: str, seller: str, link: str, wear_value: str = "", is_stattrak: bool = False):
    """
    Bargain bulunan skin için Telegram mesajı gönderir.

    Args:
        skin_name: Skin'in adı
        seller: Satıcının adı/kullanıcı adı
        link: İlana giden direkt link
        wear_value: Aşınmışlık değeri (FT, MW, vb.)
        is_stattrak: StatTrak ürün mü?
    """
    wear_tag = ""
    float_str = ""
    if wear_value:
        try:
            f = float(wear_value)
            if f < 0.07:
                wear_tag = "FN"
            elif f < 0.15:
                wear_tag = "MW"
            elif f < 0.38:
                wear_tag = "FT"
            elif f < 0.45:
                wear_tag = "WW"
            else:
                wear_tag = "BS"
            float_str = f"📏 *Float:* {_escape_markdown(wear_value)}\n🏷️ *Aşınmışlık:* {wear_tag}\n"
        except ValueError:
            # Fallback for non-float string
            float_str = f"🏷️ *Değer:* {_escape_markdown(wear_value)}\n"

    stattrak_str = "⚡ *StatTrak™*\n" if is_stattrak else ""
    message = (
        f"🎯 *BARGAIN BULUNDU!*\n\n"
        f"{stattrak_str}"
        f"🔫 *Skin:* {_escape_markdown(skin_name)}\n"
        f"{float_str}"
        f"👤 *Satıcı:* {_escape_markdown(seller)}\n"
        f"🔗 [İlana Git]({link})"
    )

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": False
    }

    try:
        response = requests.post(TELEGRAM_API_URL, json=payload, timeout=10)
        if response.status_code == 200:
            logger.info(f"Telegram bildirimi gönderildi: {skin_name}")
        else:
            logger.error(
                f"Telegram API hatası: {response.status_code} — {response.text}"
            )
    except requests.exceptions.Timeout:
        logger.error("Telegram mesajı gönderilirken zaman aşımı oluştu.")
    except requests.exceptions.ConnectionError:
        logger.error("Telegram'a bağlanılamadı. İnternet bağlantısını kontrol edin.")
    except Exception as e:
        logger.error(f"Telegram mesaj gönderim hatası: {e}")


def send_startup_message(title: str = "Cloud Sync Worker"):
    """Bot veya worker başlatıldığında bilgi mesajı gönderir."""
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": f"🤖 *{_escape_markdown(title)} başlatıldı!*\n\nİşlem döngüsü aktif...",
        "parse_mode": "Markdown"
    }
    try:
        response = requests.post(TELEGRAM_API_URL, json=payload, timeout=10)
        if response.status_code == 200:
            logger.info("Başlatma mesajı Telegram'a gönderildi.")
        else:
            logger.warning(f"Başlatma mesajı gönderilemedi: {response.status_code}")
    except Exception as e:
        logger.warning(f"Başlatma mesajı gönderilemedi: {e}")


def send_scan_error_message(skin_name: str):
    """Sıralama 'New' yapılamadığında Telegram'a hata mesajı gönderir."""
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": f"⚠️ *TARAMA HATASI!*\n\n👉 *{_escape_markdown(skin_name)}* skin'i taranırken 'New' sıralaması seçilemedi. Hatalı bargain bildirmemek için bu tarama iptal edildi.",
        "parse_mode": "Markdown"
    }
    try:
        requests.post(TELEGRAM_API_URL, json=payload, timeout=10)
    except Exception as e:
        logger.warning(f"Tarama hatası mesajı gönderilemedi: {e}")


def _escape_markdown(text: str) -> str:
    """Markdown özel karakterlerini escape eder."""
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in special_chars:
        text = text.replace(char, f"\\{char}")
    return text
