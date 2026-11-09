# -*- coding: utf-8 -*-
"""
Cloud Task Sync Worker — Konfigürasyon
Tüm sabit değerler ve ayarlar burada tanımlanır.
"""

import os

def _load_env_file():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_path):
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, val = line.split("=", 1)
                    key = key.strip()
                    val = val.strip().strip('"').strip("'")
                    if key and key not in os.environ:
                        os.environ[key] = val
        except Exception:
            pass

_load_env_file()

# ========================
# Bildirim Ayarları
# ========================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

# ========================
# Veritabanı Ayarları
# ========================
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

# ========================
# Kimlik Doğrulama Ayarları
# ========================
FB_EMAIL = os.getenv("FB_EMAIL", "")
FB_PASSWORD = os.getenv("FB_PASSWORD", "")

# ========================
# Hedef Servis URL'leri (Secret / Env üzerinden çekilir)
# ========================
TARGET_BASE_URL = os.getenv("TARGET_BASE_URL", "").rstrip("/")
MIN_PRICE_FILTER = 15
MAX_PRICE_FILTER = 1400
TARGET_LIST_URL = os.getenv(
    "TARGET_LIST_URL",
    f"{TARGET_BASE_URL}/market/all?min_price={MIN_PRICE_FILTER}&max_price={MAX_PRICE_FILTER}" if TARGET_BASE_URL else ""
)

# ========================
# İşleme Ayarları
# ========================
MAX_BARGAIN_PRICE_DIFFERENCE_PERCENT = 12.5

# Global kritik işlemler arası beklenecek rastgele sürenin sınırları (saniye)
GLOBAL_RATE_LIMIT_MIN = 3.8  # Eskisi: 3
GLOBAL_RATE_LIMIT_MAX = 5.8  # Eskisi: 5

# Detay sayfasında incelenecek maksimum kayıt sayısı
MAX_LISTINGS_TO_CHECK = 5

# Ana döngü arasındaki bekleme süresi (saniye)
CYCLE_SLEEP_MIN = 3
CYCLE_SLEEP_MAX = 6

# Sayfa yükleme bekleme süresi (saniye)
PAGE_LOAD_TIMEOUT = 20

# ========================
# Performans ve Optimizasyon
# ========================
BLOCK_MEDIA = True
BLOCK_CSS = False

# ========================
# Multi-Worker Ayarları
# ========================
MAX_WORKERS = 5 
BATCH_SIZE = 1 
SCAN_INTERVAL_MIN = 4.5      # Eskisi: 3.5 (Monitor'ün ana sayfayı tarama sıklığı)
SCAN_INTERVAL_MAX = 6.5      # Eskisi: 5

# ========================
# Tarayıcı Ayarları
# ========================
HEADLESS = True

# ========================
# Filtrelenen Anahtar Kelimeler
# ========================
BLOCKED_ITEM_KEYWORDS = ["sticker", "patch", "pin", "graffiti", "key", "pass", "music kit", "charm"]

# ========================
# Filtrelenen Kullanıcılar
# ========================
BLOCKED_SELLERS = [name.lower() for name in ["timeout", "GeRy","Beach","Mati494","eton64","nolxve","Fumbled","U55162476465","Emotionless","RBNEXT","U79042501936","Ant1ka","U57269687173","Serpent","U09127065412","U49967691992","immenz","U84224540187","GreenLine","U55162476465","player2010","FastDeliVeRy","filimon","SeRNaC","U42120220460","chubby","g1zmoo","U84414426806","U69903721068","1ncReD","FUN4EG","CaSt","TraderSuper","XOXO","QWaNZa","polococo","U53955487990","Maks69","U48521925309","U33682769020","Nast1a","TrolL1ng","grejty","U49097469990","EdgePapi","Phoenix76","sSeLwOOOOw","Kravac","Pisagor","Zicc","CYPEX","slybit","SargeeHUN","Heroxxx","U35882806666","dobrodii","Screenager","FAstdeals","prime","SBetonQS","sixue613","U45524038691","Quebeck","g3pls","ManuCS2","FriendlyF","MrSkeltal","cyhNN","waxikempire","CaZaDoR","GROMI","U94736985773","Ludicolo","GARCOLO","eVERTSON","Bence24","U64789754823","MuJi","Dariola","Mark1887","FARR","Mirrox54","U67351589045","Kapnifox","Waffu","HeyGuys","traumaticsled","U51881138201","HarryBuffAC","Zenqiii","bitPix","al1thebest","U54792677623","XERNAC","Shrederz","Szabi92","Voltiss","lkwinnr","U98555760006","U04836001751","U48240727972","fuguu666","YaroSLAVE","CuRriXx","DeathENclaimed","U71414287050","U54207007954","Fastest","Facelessness","DJENT","CheemsAV","DMTR","YAVININK","Rouling","MarkM","U87579722184","V1rdus","SHiKAKA","ngavivgg","IbzTrader","rareSkins","Zabi","Snipezzz"," U66333365844","LLIt0p0r","SGLisGOD","reymi","Darknezz","Rempy","TICHIGO","U99717327342","TrapezZ","U70312132636","U65869018171","Jaaaps","Joelssj","XPDEUSII","Ow3n","Bonanza","JasPi","Bruhn","reister","F1dF","Allstardrew21","DobbySock","DMTR","Zabi","b0ZaiNikA","Snipezzz","U60407570908","V1rdus","MonkeyMarket","LaMaT","Traplex008","U45277440131","Real100deliver","klowisSB"]]
