import logging
import requests
import random

logger = logging.getLogger(__name__)

def get_free_proxies() -> list:
    logger.info("[ProxyManager] Ücretsiz proxy listesi çekiliyor...")
    url = "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=all&ssl=all&anonymity=all"
    try:
        response = requests.get(url, timeout=10)
        proxies = response.text.strip().split('\r\n')
        # Boş elemanları temizle
        proxies = [p for p in proxies if p]
        logger.info(f"[ProxyManager] {len(proxies)} adet proxy bulundu.")
        return proxies
    except Exception as e:
        logger.error(f"[ProxyManager] Proxy listesi alınamadı: {e}")
        return []

def get_working_proxy(proxies: list) -> str:
    """Proxy listesini dolaşır ve IPify üzerinden çalışan ilk proxy'yi döndürür."""
    # Worker'ların aynı anda başlatılıp aynı listeyi sırayla işlediğinde aynı proxy'ye 
    # yığılmasını engellemek için listeyi karıştırıyoruz.
    random.shuffle(proxies)
    
    for proxy in proxies:
        proxies_dict = {
            "http": f"http://{proxy}",
            "https": f"http://{proxy}"
        }
        try:
            logger.debug(f"Test ediliyor: {proxy}")
            res = requests.get("https://api.ipify.org?format=json", proxies=proxies_dict, timeout=5)
            if res.status_code == 200:
                logger.info(f"[ProxyManager] ✓ Çalışan Proxy Bulundu: {proxy}")
                return proxy
        except Exception:
            continue
            
    logger.warning("[ProxyManager] ❌ Çalışan proxy bulunamadı.")
    return None
