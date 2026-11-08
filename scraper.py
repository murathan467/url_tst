# -*- coding: utf-8 -*-
"""
Cloud Task Sync — Web Parser Modülü
Async Playwright ve stealth kullanılarak yazılmıştır.
"""

import os
import asyncio
import time
import random
import logging

from playwright.async_api import async_playwright, Page, BrowserContext, TimeoutError as PlaywrightTimeoutError

import config

logger = logging.getLogger(__name__)

# Stealth kütüphanesini dışarıdan deniyoruz (yoksa normal devam eder)
try:
    from playwright_stealth import Stealth
    STEALTH_PROXY_AVAILABLE = True
except ImportError as e:
    try:
        from playwright_stealth import stealth_async
        STEALTH_PROXY_AVAILABLE = 'LEGACY'
    except ImportError as e2:
        STEALTH_PROXY_AVAILABLE = False
        logger.warning(f"playwright-stealth yüklenemedi! Bot algılanma ihtimali çok yüksek. Lütfen 'python -m pip install playwright-stealth' komutunu çalıştırın.")

class DataScraper:
    """Hedef servis üzerinde kayıt tarayan asenkron scraper sınıfı."""

    def __init__(self):
        self.playwright = None
        self.browser = None
        self.context: BrowserContext = None
        self.action_lock = asyncio.Lock()
        self.last_action_time = 0.0
        # Global cooldown: Toast rate limit yakalandığında TÜM worker'lar bu zamana kadar bekler
        self.global_cooldown_until = 0.0

    async def wait_for_rate_limit(self, action_name: str = "Aksiyon"):
        """Tüm botların ortaklaşa kullandığı global limit kontrolü."""
        # Önce global cooldown kontrolü (toast rate limit sonrası)
        now = time.time()
        if self.global_cooldown_until > now:
            wait_time = self.global_cooldown_until - now
            logger.warning(f"[Global Cooldown] 🛑 Rate limit cooldown aktif! '{action_name}' {wait_time:.1f} sn bekleyecek...")
            await asyncio.sleep(wait_time)
        
        # Sabit sürelere karşı WAF yakalanmasını önlemek için rastgelelik
        limit_min = getattr(config, "GLOBAL_RATE_LIMIT_MIN", 1.0)
        limit_max = getattr(config, "GLOBAL_RATE_LIMIT_MAX", 3.0)
        
        delay = random.uniform(limit_min, limit_max)
        
        async with self.action_lock:
            now = time.time()
            if self.last_action_time > now:
                wait_time = self.last_action_time - now
                logger.warning(f"[Global Rate Limit] 🛑 Global duraklatma aktif! '{action_name}' islemi {wait_time:.1f} sn ertelendi.")
                await asyncio.sleep(wait_time)
                self.last_action_time = time.time()
            else:
                elapsed = now - self.last_action_time
                if elapsed < delay:
                    wait_time = delay - elapsed
                    logger.info(f"[Global Rate Limit] ⏳ Baska bir bot islem yapti. '{action_name}' icin {wait_time:.1f} sn bekleniyor...")
                    await asyncio.sleep(wait_time)
                self.last_action_time = time.time()

    async def check_and_handle_toast(self, page: Page):
        """Ekranda hata toast mesajı var mı diye hızlıca bakar, varsa TÜM worker'ları global olarak durdurur."""
        try:
            toast = page.locator(".p-toast--error").first
            # Hatalar genellikle işlemden hemen sonra belirir, hızlıca bekle
            await toast.wait_for(state="visible", timeout=800)
            cooldown_seconds = 15.0
            self.global_cooldown_until = time.time() + cooldown_seconds
            logger.warning(f"[Global Cooldown] 🛑 Rate limit (Toast) yakalandi! TÜM worker'lar {cooldown_seconds:.0f} saniye duraklatılıyor...")
            await asyncio.sleep(cooldown_seconds)
        except PlaywrightTimeoutError:
            pass
        except Exception as e:
            logger.debug(f"Toast kontrol hatasi: {e}")

    async def start_browser(self, proxy_url: str = None) -> BrowserContext:
        """Playwright chromium asenkron olarak ayağa kalkar ve bir kalıcı Context başlatır/döndürür."""
        try:
            self.playwright = await async_playwright().start()
            
            profile_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ChromeProfilePlaywright")
            os.makedirs(profile_dir, exist_ok=True)
            
            args = [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-popup-blocking",
                "--lang=en-US"
            ]
            
            context_args = {
                "user_data_dir": profile_dir,
                "headless": config.HEADLESS,
                "viewport": {"width": 1920, "height": 1080},
                "args": args,
                "ignore_https_errors": True
            }
            if proxy_url:
                context_args["proxy"] = {"server": proxy_url}
                logger.info(f"Proxy atanıyor: {proxy_url}")

            self.context = await self.playwright.chromium.launch_persistent_context(**context_args)
            
            # Kaynak engelleme (Resim, Medya, Font ve OPSIYONEL OLARAK CSS)
            block_types = []
            if getattr(config, "BLOCK_MEDIA", True):
                block_types.extend(["image", "media", "font"])
            if getattr(config, "BLOCK_CSS", False):
                block_types.append("stylesheet")
                
            if block_types:
                async def block_resources(route):
                    if route.request.resource_type in block_types:
                        await route.abort()
                    else:
                        await route.continue_()
                await self.context.route("**/*", block_resources)
                logger.info(f"Performans optimizasyonu aktif. Engellenen tipler: {', '.join(block_types)}")
            
            if STEALTH_PROXY_AVAILABLE == True:
                try:
                    await Stealth().apply_stealth_async(self.context)
                    logger.info("Playwright Stealth kalkanı tüm sekmelere uygulandı.")
                except Exception as e:
                    logger.warning(f"Stealth kalkanı başlatılırken hata: {e}")
            elif STEALTH_PROXY_AVAILABLE == 'LEGACY':
                logger.warning("Eski sürüm playwright-stealth algılandı. Lütfen pip ile güncelleyin.")
            
            logger.info("Playwright tarayıcısı başarıyla başlatıldı.")
            return self.context
            
        except Exception as e:
            logger.error(f"Tarayıcı başlatma hatası: {e}")
            raise

    async def close_browser(self):
        """Playwright bağlamı kapatılır."""
        if self.context:
            try:
                await self.context.close()
            except Exception:
                pass
        if self.playwright:
            try:
                await self.playwright.stop()
            except Exception:
                pass
        logger.info("Tarayıcı kapatıldı.")

    async def _cleanup_popups(self, page: Page):
        """Sayfa içi popupları ve diyalog pencerelerini yönetir."""
        close_selectors = [
            "div.w-dialog__headerbtn",
            "button.w-dialog__headerbtn",
            ".modal-close",
            "button[class*='close']",
            "div[class*='w-dialog'] button"
        ]
        for sel in close_selectors:
            try:
                btns = await page.locator(sel).all()
                for btn in btns:
                    if await btn.is_visible():
                        await btn.click(force=True)
                        await asyncio.sleep(0.5)
            except Exception:
                pass

    async def _scroll_slowly(self, page: Page):
        """Asenkron yavaş kaydırma metodu."""
        try:
            for _ in range(3):
                amount = random.randint(400, 800)
                await page.evaluate(f"window.scrollBy(0, {amount});")
                await asyncio.sleep(random.uniform(0.5, 1.0))
        except Exception:
            pass

    async def fetch_skin_list(self, page: Page, last_scanned_id: str = "") -> tuple:
        """Monitor görevini yöneten metod: Ana item listesini okuyup döndürür.
           Ayrıca döngünün ilk okuduğu öğeyi history için geri döndürür."""
        skins = []
        first_item_id = ""
        first_item_name = ""
        try:
            logger.info(f"Ana liste sayfasına gidiliyor: {config.TARGET_LIST_URL}")
            await self.wait_for_rate_limit("Ana sayfa taranıyor")
            await page.goto(config.TARGET_LIST_URL, timeout=config.PAGE_LOAD_TIMEOUT * 1000)
            await self.check_and_handle_toast(page)
            await asyncio.sleep(random.uniform(2, 4))
            
            await self._cleanup_popups(page)
            await self._scroll_slowly(page)
            await asyncio.sleep(1)

            # Linkleri Playwright locator ile bul
            item_links = await page.locator("a[href*='/market/goods/']").all()
            if not item_links:
                logger.warning("Hiçbir skin kartı bulunamadı.")
                return skins
                
            seen_ids = set()
            for item in item_links:
                try:
                    href = await item.get_attribute("href")
                    if not href: continue
                    
                    skin_id = self._extract_goods_id(href)
                    if not skin_id or skin_id in seen_ids:
                        continue
                        
                    name = ""
                    # H3 etiketini içinden bulmayı dene
                    h3s = await item.locator("h3").all()
                    if h3s:
                        name = await h3s[0].get_attribute("title")
                        if not name:
                            name = await h3s[0].inner_text()
                    
                    if not name:
                         name = await item.inner_text()
                    
                    name = name.strip()
                    if not name: name = f"Skin_{skin_id}"

                    if skin_id == last_scanned_id:
                        logger.info(f"⏭ Daha önce taranan limite gelindi (ID: {skin_id} - İsim: {name}). Sayfanın kalanı atlanıyor.")
                        break
                        
                    if not first_item_id:
                        first_item_id = skin_id
                        first_item_name = name
                    
                    # config içerisindeki yasaklı kelimeleri kontrol et
                    skip_item = False
                    for keyword in getattr(config, "BLOCKED_ITEM_KEYWORDS", ["sticker"]):
                        if keyword in name.lower():
                            skip_item = True
                            break
                    if skip_item:
                        continue
                    
                    wear_value = ""
                    try:
                        full_text = await item.inner_text()
                        full_text_lower = full_text.lower()
                        name_lower = name.lower()
                        words = full_text.split()
                        
                        if "factory new" in full_text_lower or "factory new" in name_lower or "FN" in words:
                            wear_value = "FN"
                        elif "minimal wear" in full_text_lower or "minimal wear" in name_lower or "MW" in words:
                            wear_value = "MW"
                        elif "field-tested" in full_text_lower or "field-tested" in name_lower or "FT" in words:
                            wear_value = "FT"
                        elif "well-worn" in full_text_lower or "well-worn" in name_lower or "WW" in words:
                            wear_value = "WW"
                        elif "battle-scarred" in full_text_lower or "battle-scarred" in name_lower or "BS" in words:
                            wear_value = "BS"
                        elif "vanilla" in full_text_lower or "vanilla" in name_lower or "Vanilla" in words:
                            wear_value = "Vanilla"
                    except Exception:
                        pass
                    
                    seen_ids.add(skin_id)
                    skins.append({
                        "id": skin_id,
                        "name": name,
                        "url": href if href.startswith("http") else f"{config.TARGET_BASE_URL}{href}",
                        "wear_value": wear_value
                    })
                except Exception as e:
                    pass
            
            logger.info(f"Ana listeden {len(skins)} adet benzersiz yeni skin çekildi.")
            
        except PlaywrightTimeoutError:
            logger.error("Ana sayfa yüklenirken zaman aşımı yaşandı.")
        except Exception as e:
            logger.error(f"fetch_skin_list hatası: {e}")
            
        return skins, first_item_id, first_item_name

    async def _set_sort_to_newest(self, page: Page):
        """Detay sayfasındaki sıralama dropdown'ını 'New' olarak ayarlar."""
        try:
            # Önce popup'ları kapat
            await self._cleanup_popups(page)
            
            # Dropdown trigger: .select-quality (sıralama butonu)
            dropdown = page.locator(".select-quality")
            
            # Dropdown görünür mü kontrol et
            try:
                await dropdown.wait_for(state="visible", timeout=5000)
            except PlaywrightTimeoutError:
                logger.debug("Sıralama dropdown'ı bulunamadı, atlanıyor.")
                return False
            
            # Zaten "New" seçili mi kontrol et
            try:
                current_text = await dropdown.inner_text()
                if "new" in current_text.lower():
                    logger.debug("Sıralama zaten 'New' olarak ayarlı.")
                    return True
            except Exception:
                pass
            
            # Dropdown'ı aç
            await dropdown.click(force=True)
            await asyncio.sleep(1.0)
            
            # Playwright native locator ile "New" seçeneğini bul ve tıkla
            try:
                # Body'nin sonuna eklenen görünür (portal/popper) w-select-dropdown içindeki "New" seçeneğini bul
                new_option = page.locator(".w-select-dropdown:visible .select-option", has_text="New").first
                await new_option.wait_for(state="visible", timeout=3000)
                await self.wait_for_rate_limit("'New' butonu tıklanıyor")
                await new_option.click(force=True)
                await self.check_and_handle_toast(page)
                logger.debug("Sıralama 'New' olarak değiştirildi.")
                await asyncio.sleep(1.5)  # Listenin yeniden yüklenmesini bekle
                return True
            except Exception as e:
                logger.debug(f"'New' seçeneği bulunamadı veya tıklanamadı: {e}")
                await page.keyboard.press("Escape")
                return False
                
        except Exception as e:
            logger.debug(f"Sıralama değiştirme hatası: {e}")
            try:
                await page.keyboard.press("Escape")
            except Exception:
                pass
            return False

    async def check_detail_page_for_bargain(self, page: Page, skin_url: str, skin_name: str) -> list:
        """Worker'ların kullanacağı tekil ürün tarama fonksiyonu."""
        bargain_results = []
        try:
            logger.info(f"[Worker] Detay sayfası işleniyor: {skin_name}")
            
            # Retry mekanizması: 2 deneme
            page_loaded = False
            for attempt in range(2):
                try:
                    await self.wait_for_rate_limit(f"Detay sayfası yükleniyor: {skin_name}")
                    await page.goto(skin_url, timeout=config.PAGE_LOAD_TIMEOUT * 1000)
                    await self.check_and_handle_toast(page)
                    page_loaded = True
                    break
                except PlaywrightTimeoutError:
                    if attempt == 0:
                        logger.warning(f"[Worker] '{skin_name}' sayfa yükleme timeout, tekrar deneniyor...")
                        try:
                            await page.reload(timeout=config.PAGE_LOAD_TIMEOUT * 1000)
                            await self.check_and_handle_toast(page)
                            page_loaded = True
                            break
                        except PlaywrightTimeoutError:
                            pass
                    logger.warning(f"[Worker] '{skin_name}' tüm denemeler başarısız, TimeoutError fırlatılıyor.")
                    raise Exception("Timeout Error: Sayfa yüklemesi başarısız, proxy yavaş ya da ölü!")
            
            if not page_loaded:
                return bargain_results
            
            content = await page.content()
            title = await page.title()
            if "Access denied" in content or "Cloudflare" in title or "Just a moment..." in title:
                raise Exception("WAF Engeli veya Captcha yakalandı!")

            await asyncio.sleep(random.uniform(2, 4))
            await self._cleanup_popups(page)
            
            # Sıralamayı 'New' olarak ayarla
            sort_success = await self._set_sort_to_newest(page)
            if sort_success is False:
                logger.warning(f"[Worker] '{skin_name}' için sıralama 'New' yapılamadı, atlanıyor.")
                return [{"error": "sort_failed", "skin_name": skin_name}]
            
            await self._scroll_slowly(page)
            
            # "Buy" butonlarının belirmesini bekle
            try:
                await page.locator("button:has-text('Buy'), button:has-text('BUY')").first.wait_for(state="visible", timeout=12000)
            except PlaywrightTimeoutError:
                logger.debug(f"[Worker] '{skin_name}' için Buy butonları zamanında yüklenemedi.")

            # Ana sayfa (referans) fiyatını çek
            reference_price = 0.0
            try:
                reference_price = await page.evaluate(r'''() => {
                    let rp = 0.0;
                    
                    // 1. Olasılık: Bilindik class veya ID
                    let pEl = document.querySelector(".goods-info .price, .detail-header .price, [class*='price']");
                    if (pEl) {
                        let text = pEl.innerText || "";
                        if(text.includes('$') || text.includes('¥') || text.includes('€') || text.includes('RMB') || /[0-9]/.test(text)) {
                            let match = text.replace(/,/g, '').match(/[\d.]+/);
                            if(match) rp = parseFloat(match[0]);
                        }
                    }
                    if (rp > 0) return rp;
                    
                    // 2. Olasılık: Referans veya Steam fiyatı etiketinin yanındaki fiyatı ara
                    let labels = document.querySelectorAll("span, div, p");
                    for(let el of labels) {
                        let t = el.innerText || "";
                        if(t.toLowerCase().includes("reference") || t.toLowerCase().includes("steam") || t.toLowerCase().includes("referans")) {
                            let parentText = (el.parentElement ? el.parentElement.innerText : t);
                            let match = parentText.replace(/,/g, '').match(/[\d.]+/);
                            if(match) {
                                let val = parseFloat(match[0]);
                                if (val > 0) return val;
                            }
                        }
                    }
                    
                    // 3. Fallback: Sayfadaki ilk mantıklı geçerli sembollü fiyatı bul
                    for(let el of labels) {
                        let text = (el.innerText || "").trim();
                        if(text.includes('$') || text.includes('¥') || text.includes('€') || text.includes('RMB')) {
                            let match = text.replace(/,/g, '').match(/[\d.]+/);
                            if(match) {
                                let val = parseFloat(match[0]);
                                if (val > 0 && !text.startsWith("0.0")) return val;
                            }
                        }
                    }
                    return 0.0;
                }''')
                
                # JavaScript'ten string vb gelebilir, güvenlik:
                reference_price = float(reference_price) if reference_price else 0.0
            except Exception as e:
                logger.debug(f"[Worker] '{skin_name}' için referans fiyat alınamadı: {e}")
                reference_price = 0.0
                
            # DOM üzerinden row tespiti
            buy_buttons = await page.locator("button:has-text('Buy'), button:has-text('BUY')").all()
            
            rows = []
            for btn in buy_buttons:
                if await btn.is_visible():
                    row_el = await btn.evaluate_handle('''btn => {
                        let attempt = btn;
                        while(attempt && attempt.tagName !== 'BODY') {
                            if(attempt.className && typeof attempt.className === 'string' && attempt.className.includes('market-goods-sell-content')) {
                                return attempt;
                            }
                            attempt = attempt.parentElement;
                        }
                        return btn.parentElement.parentElement.parentElement;
                    }''')
                    if row_el and row_el not in [r["handle"] for r in rows]:
                        rows.append({"handle": row_el, "btn_locator": btn})

            if not rows:
                return bargain_results
                
            valid_listings_checked = 0
            
            for i, row_data in enumerate(rows):
                if valid_listings_checked >= config.MAX_LISTINGS_TO_CHECK:
                    break
                try:
                    row_handle = row_data["handle"]
                    
                    # Satıcı, Wear Value ve StatTrak bilgilerini tek seferde çek
                    row_info = await row_handle.evaluate(r'''row => {
                        // --- Satıcı Adı ---
                        let seller = "Bilinmeyen Satıcı";
                        let nameEls = row.querySelectorAll(".market-goods-sell-col-seller-name, .market-goods-sell-col-seller");
                        for(let el of nameEls) {
                            if(el.innerText) { seller = el.innerText.trim(); break; }
                        }
                        if(seller === "Bilinmeyen Satıcı") {
                            let spans = row.querySelectorAll("span");
                            for(let span of spans) {
                                let text = span.innerText.trim();
                                if(text && !text.startsWith("0.") && !text.startsWith("$") && text.length > 2 && text.toLowerCase().indexOf('new') === -1) {
                                    seller = text; break;
                                }
                            }
                        }
                        
                        // --- Wear Value (Sadece Float) ---
                        let wearValue = "";
                        
                        // 1. Tüm span, div ve paragraflarda 0. başla ve en az iki hanesi olan kısmı ara
                        let allSpans = row.querySelectorAll("span, div, p");
                        for(let sp of allSpans) {
                            let t = sp.innerText.trim();
                            let match = t.match(/0\.\d{2,}/);
                            if(match) {
                                wearValue = match[0];
                                break;
                            }
                        }
                        
                        // 2. Eğer elementler içinde net bulunamadıysa satırın isimsiz tüm metni içinde ara
                        if(!wearValue) {
                            let fullText = row.innerText || "";
                            let match = fullText.match(/0\.\d{2,}/);
                            if(match) {
                                wearValue = match[0];
                            }
                        }
                        
                        // --- StatTrak ---
                        let isStatTrak = false;
                        let fullRowText = row.innerText || "";
                        if(fullRowText.includes("StatTrak") || fullRowText.includes("stattrak") || fullRowText.includes("STATTRAK")) {
                            isStatTrak = true;
                        }
                        // Ayrıca StatTrak class veya ikon kontrolü
                        let stEls = row.querySelectorAll("[class*='stattrak'], [class*='StatTrak'], .stattrak-icon");
                        if(stEls.length > 0) isStatTrak = true;
                        
                        // --- Fiyat (Listing Price) ---
                        let listingPrice = 0.0;
                        let priceEls = row.querySelectorAll(".price p, strong.f_Strong, td.market-goods-sell-col-price strong, .price string, strong");
                        for(let el of priceEls) {
                            let pt = (el.innerText || "").trim();
                            if(pt.includes('$') || pt.includes('¥') || pt.includes('€') || pt.includes('RMB')) {
                                let match = pt.replace(/,/g, '').match(/[\d.]+/);
                                if(match) {
                                    listingPrice = parseFloat(match[0]);
                                    break;
                                }
                            }
                        }
                        
                        if(listingPrice === 0.0) {
                            // Fallback: Satırdaki tüm metni "\n" ile ayırıp fiyat sembolü arıyoruz.
                            let allTextLines = (row.innerText || "").split('\n');
                            for(let pt of allTextLines) {
                                pt = pt.trim();
                                if(pt.includes('$') || pt.includes('¥') || pt.includes('€') || pt.includes('RMB')) {
                                    let match = pt.replace(/,/g, '').match(/[\d.]+/);
                                    if(match) {
                                        let parsed = parseFloat(match[0]);
                                        // Float değerleri ya da çok küçük değerleri fiyat sanmaması için güvenlik
                                        if (parsed > 0.01 && !pt.startsWith("0.0")) {
                                            listingPrice = parsed;
                                            break;
                                        }
                                    }
                                }
                            }
                        }

                        // --- Bargain Kontrolü ---
                        let hasBargain = false;
                        let btns = row.querySelectorAll("button.bargain-btn");
                        for(let b of btns) {
                            let style = window.getComputedStyle(b);
                            if(style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0' && b.className.indexOf('disable') === -1) {
                                let rect = b.getBoundingClientRect();
                                if(rect.width > 0 && rect.height > 0) { hasBargain = true; break; }
                            }
                        }
                        
                        return { seller, wearValue, isStatTrak, hasBargain, listingPrice };
                    }''')
                    
                    seller = row_info.get("seller", "Bilinmeyen Satıcı")
                    wear_value = row_info.get("wearValue", "")
                    is_stattrak = row_info.get("isStatTrak", False)
                    has_bargain = row_info.get("hasBargain", False)
                    listing_price = row_info.get("listingPrice", 0.0)
                    
                    # Eğer tamamen boş ve anlamsız bir satır geldiyse (örn. navbardaki Buy butonu), sayma ve atla
                    if seller == "Bilinmeyen Satıcı" and listing_price == 0.0:
                        continue
                        
                    valid_listings_checked += 1
                    
                    if seller.lower() == "market" or "inventory" in seller.lower():
                        continue
                    
                    # Skin adından da StatTrak kontrolü (satır DOM'da yoksa)
                    if not is_stattrak and "stattrak" in skin_name.lower():
                        is_stattrak = True
                    
                    if has_bargain:
                        skip_because_expensive = False
                        if reference_price > 0 and listing_price > 0:
                            max_diff_percent = getattr(config, "MAX_BARGAIN_PRICE_DIFFERENCE_PERCENT", 12.5)
                            diff_percent = ((listing_price - reference_price) / reference_price) * 100
                            if diff_percent > max_diff_percent:
                                logger.info(f"⏭️ Bargain atlandı: {skin_name} (Satıcı: {seller}). İlan: {listing_price}, Normal: {reference_price}. Fark: +%{diff_percent:.1f} (> %{max_diff_percent})")
                                skip_because_expensive = True
                        
                        if not skip_because_expensive:
                            bargain_results.append({
                                "skin_name": skin_name,
                                "seller": seller,
                                "link": skin_url,
                                "wear_value": wear_value,
                                "is_stattrak": is_stattrak
                            })
                            logger.info(f"🎉 BARGAIN BULUNDU! Skin: {skin_name}, Satıcı: {seller}, İlan Fiyatı: {listing_price}, Wear: {wear_value}, StatTrak: {is_stattrak}")
                            
                except Exception as e:
                    logger.debug(f"[Worker] Satır parse hatası: {e}")

            # Geri kalan kullanılmayan evaluate objelerini dispose edelim (memory leak önlemi)
            for r in rows:
                await r["handle"].dispose()

        except PlaywrightTimeoutError:
            logger.warning(f"Detay sayfası timeout ({skin_name}), ürün atlanıyor.")
        except Exception as e:
            logger.error(f"Detay sayfası hatası ({skin_name}): {e}")
            
        return bargain_results

    @staticmethod
    def _extract_goods_id(url: str) -> str:
        """URL'den goods_id çıkarır."""
        if not url: return ""
        try:
            if "/market/goods/" in url:
                parts = url.split("/market/goods/")
                if len(parts) > 1:
                    goods_part = parts[1].split("?")[0].split("#")[0]
                    goods_part = goods_part.strip("/")
                    if goods_part: return goods_part
            if "goods_id=" in url:
                parts = url.split("goods_id=")
                if len(parts) > 1:
                    gid = parts[1].split("&")[0].split("#")[0]
                    if gid: return gid
        except Exception:
            pass
        return ""
