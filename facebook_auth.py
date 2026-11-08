import time
import json
import os
import logging
from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

import asyncio

logger = logging.getLogger(__name__)

COOKIES_FILE = "session_cookies.json"

async def handle_facebook_login(page: Page):
    """
    Oturum pop-up'ını ve harici sekmedeki giriş akışını (Playwright ile) yönetir.
    Ayrıca başarılı girişte cookieleri kaydeder, hata durumunda eski cookielere geri döner.
    """
    logger.info("[AUTH] Cookie kontrolü yapılıyor...")
    
    # 1. Kaydedilmiş cookie varsa önce onu sisteme yükle
    if os.path.exists(COOKIES_FILE):
        try:
            with open(COOKIES_FILE, "r") as f:
                cookies = json.load(f)
            await page.context.add_cookies(cookies)
            logger.info("[AUTH] Kaydedilmiş cookieler yüklendi. Durum tespiti için sayfa yenileniyor...")
            await page.reload()
            await asyncio.sleep(3)
        except Exception as e:
            logger.warning(f"[AUTH] Cookieler yüklenirken hata: {e}")

    # 2. Giriş yapılmış mı kontrol et ("Sign in" butonu yoksa giriş yapılmıştır)
    try:
        sign_btn = page.locator("button.c-nav-sign").first
        await sign_btn.wait_for(state="hidden", timeout=5000)
        logger.info("[AUTH] Zaten giriş yapılmış durumda (Sign In butonu yok!). Facebook akışı atlanıyor.")
        
        # Mevcut başarılı durumu da yedekleyelim
        try:
            current_cookies = await page.context.cookies()
            with open(COOKIES_FILE, "w") as f:
                json.dump(current_cookies, f)
        except Exception:
            pass
        return  # Akışı burada kes
    except Exception:
        logger.info("[AUTH] Sign In butonu ekranda mevcut. Login işlemi başlatılıyor...")

    logger.info("[AUTH] Sign In akışı başlatılıyor...")
    
    # 1. Ana sayfadaki "Sign in" butonuna tıkla (Eğer henüz açılmadıysa)
    try:
        sign_in_locators = [
            "button.c-nav-sign",
            "button:has-text('Sign in')",
            "text='Sign in'",
            "text='Sign In'"
        ]
        
        btn_clicked = False
        for loc in sign_in_locators:
            elements = await page.locator(loc).all()
            for el in elements:
                if await el.is_visible():
                    logger.info(f"[AUTH] Ana sayfada 'Sign in' butonu bulundu ({loc}), tıklanıyor...")
                    try:
                        await el.click(force=True, timeout=2000)
                    except Exception as click_err:
                        logger.debug(f"[AUTH] Standart tıklama hatası: {click_err}, JS click deneniyor...")
                        await el.evaluate("node => node.click()")
                        
                    btn_clicked = True
                    break
            if btn_clicked:
                break
                
        if btn_clicked:
            await asyncio.sleep(2)
            
    except Exception as e:
        logger.debug(f"[AUTH] Ana sayfadaki Sign In butonu tıklanamadı: {e}")

    fb_clicked = False
    
    # 2. Login modalının açılmasını bekle
    try:
        modal_locator = page.locator(".w-dialog, .v-modal, [role='dialog'], .login-dialog")
        await modal_locator.first.wait_for(state="visible", timeout=5000)
        await asyncio.sleep(1) # Modal animasyonu için
        logger.info("[AUTH] Login modalı ekranda göründü.")
    except Exception:
        logger.debug("[AUTH] Login modalı tespit edilemedi, ekranda aramaya devam edilecek.")

    # 3. Modal içindeki Facebook giriş butonunu bul ve tıkla
    try:
        fb_selectors_page = [
            "a[href='/account/login/facebook']",
            "a[href*='login/facebook']",
            "a.is-facebook",
            ".login-action-item-wrap.is-facebook",
        ]
        
        fb_selectors_iframe = [
            ".login-facebook",
            ".is-facebook",
            "div:has-text('Facebook')",
        ]

        logger.info("[AUTH] Facebook butonları aranıyor...")
        # 5 saniye boyunca elementi bulana kadar tekrar dene (tam yüklenmesi için zaman tanı)
        for _ in range(5):
            
            # 3.1. Önce doğrudan iframe içinde ara (Unisdk Easebar)
            try:
                # Playwright's frame_locator penetrates cross-origin frames cleanly 
                frame = page.frame_locator('iframe[src*="unisdk.easebar.com"]')
                for sel in fb_selectors_iframe:
                    if await frame.locator(sel).count() > 0 and await frame.locator(sel).first.is_visible():
                        logger.warning(f"[AUTH] Iframe içinde Facebook butonu ({sel}) bulundu, tıklanıyor...")
                        async with page.context.expect_page() as new_page_info:
                            await frame.locator(sel).first.click(force=True)
                        new_page = await new_page_info.value
                        await new_page.wait_for_load_state()
                        fb_clicked = True
                        break
            except Exception as e:
                pass
                
            if fb_clicked: break
            
            # 3.2. Iframe'de bulunamazsa ana sayfadaki locators üzerinden dene
            for sel in fb_selectors_page:
                try:
                    elements = await page.locator(sel).all()
                    for el in elements:
                        if await el.is_visible():
                            logger.warning(f"[AUTH] Normal sayfada Facebook butonu ({sel}) bulundu, tıklanıyor...")
                            async with page.context.expect_page() as new_page_info:
                                await el.click(force=True)
                            
                            new_page = await new_page_info.value
                            await new_page.wait_for_load_state()
                            fb_clicked = True
                            break
                except Exception:
                    pass
                if fb_clicked: break
            
            if fb_clicked: break
            await asyncio.sleep(1)
            
        if not fb_clicked:
            logger.debug("[AUTH] 5 saniye içinde Facebook butonu ne ana sayfada ne de iframelerde bulunamadı.")
            
    except PlaywrightTimeoutError:
         logger.debug("[AUTH] Facebook Pop-up tetikleme zaman aşımına uğradı.")
    except Exception as e:
        logger.error(f"[AUTH] Facebook giriş butonuna tıklarken hata: {e}")

    # 4. Açılan yeni Facebook sekmesine (new_page) geç ve işlemleri tamamla
    if fb_clicked and 'new_page' in locals():
        logger.info("[AUTH] Facebook sekmesinin açılması bekleniyor...")
        await asyncio.sleep(3) 
        
        try:
            logger.info("[AUTH] Facebook sekmesine geçildi. Sayfanın yüklenmesi bekleniyor...")
            
            # Çerez (Cookie) Duvarını Geç
            try:
                cookie_selectors = [
                    "button:has-text('Temel ve isteğe bağlı çerezlere izin ver')",
                    "button:has-text('Allow essential and optional cookies')",
                    "button:has-text('Allow all cookies')",
                    "button:has-text('İzin Ver')",
                    "[data-testid='cookie-policy-manage-dialog-accept-button']"
                ]
                for cx in cookie_selectors:
                    c_btns = await new_page.locator(cx).all()
                    for cb in c_btns:
                        if await cb.is_visible():
                            await cb.click(force=True)
                            logger.info("[AUTH] Facebook çerez (cookie) duvarı geçildi.")
                            await asyncio.sleep(2)
                            break
                    else:
                        continue
                    break
            except Exception as ce:
                logger.debug(f"[AUTH] Çerez butonu aranırken hata: {ce}")
            
            # A: Önce "Murathan Olarak Devam Et" (Continue as) butonunu ara
            try:
                continue_selectors = [
                    "div[aria-label*='Devam']",
                    "div[aria-label*='Continue']",
                    "div[aria-label*='olarak devam et' i]",
                    "span:has-text('Devam')",
                    "span:has-text('olarak devam et')",
                    "span:has-text('Continue as')",
                    "div[role='button']:has-text('Devam')",
                    "div[role='button']:has-text('Continue')",
                    "div[role='button']:has-text('olarak devam et')",
                    "button:has-text('Devam')",
                    "button:has-text('Continue')",
                    "div[aria-label*='Murathan olarak devam et']"
                ]
                
                clicked_continue = False
                for sel in continue_selectors:
                    btns = await new_page.locator(sel).all()
                    for btn in btns:
                        if await btn.is_visible():
                            try:
                                await btn.click(force=True)
                                logger.info("[AUTH] Facebook 'Devam Et' butonuna tıklandı.")
                                clicked_continue = True
                                break
                            except Exception:
                                continue
                    if clicked_continue: break
                            
                if not clicked_continue:
                    # B: Devam butonu yoksa Email/Şifre formunu doldur
                    logger.info("[AUTH] Facebook login formu dolduruluyor...")
                    from config import FB_EMAIL, FB_PASSWORD
                    
                    email_inputs = await new_page.locator("input[name='email'], input[placeholder*='E-posta'], input[placeholder*='email']").all()
                    if email_inputs and FB_EMAIL:
                        await email_inputs[0].fill(FB_EMAIL)
                        await asyncio.sleep(0.5)
                        
                    pass_inputs = await new_page.locator("input[name='pass'], input[type='password']").all()
                    if pass_inputs and FB_PASSWORD:
                        await pass_inputs[0].fill(FB_PASSWORD)
                        await asyncio.sleep(0.5)
                        try:
                            # Butonu bulamama ihtimaline karşı doğrudan Enter'a basalım
                            await pass_inputs[0].press("Enter")
                            logger.info("[AUTH] Enter tuşu ile form tetiklendi.")
                        except Exception:
                            pass
                    
                    submit_selectors = [
                        "button[name='login']",
                        "button[id='loginbutton']",
                        "#loginbutton",
                        "button[type='submit']",
                        "[data-testid='royal_login_button']",
                        "div[aria-label='Giriş Yap']",
                        "div[aria-label='Log In']",
                        "button:has-text('Giriş Yap')",
                        "button:has-text('Log In')",
                        "button:has-text('Log in')",
                        "input[name='login']",
                        "input[type='submit']"
                    ]
                    submit_clicked = False
                    for s_sel in submit_selectors:
                        submit_btns = await new_page.locator(s_sel).all()
                        for sm in submit_btns:
                             if await sm.is_visible():
                                 try:
                                     await sm.click(force=True)
                                     submit_clicked = True
                                     break
                                 except Exception:
                                     pass
                        if submit_clicked: break
                    
                    logger.info("[AUTH] Facebook hesap bilgileri ile login butonuna tıklandı.")
                    
                    # 3. Şifreyle girdikten sonra Facebook, "Devam et" diyebilir. Onu da bekle.
                    logger.info("[AUTH] Şifre sonrası 'Devam Et' ekranı bekleniyor...")
                    await asyncio.sleep(5)
                    try:
                        for sel in continue_selectors:
                            after_btns = await new_page.locator(sel).all()
                            for btn in after_btns:
                                if await btn.is_visible():
                                    await btn.click(force=True)
                                    logger.info("[AUTH] Şifre girişi sonrası Facebook 'Devam Et' butonuna tıklandı.")
                                    break
                    except Exception as e_continue:
                        logger.debug(f"[AUTH] Şifre sonrası devam et bulunamadı: {e_continue}")

            except Exception as try_e:
                logger.warning(f"[AUTH] Facebook form içi doldurmada hata: {try_e}")
            logger.info("[AUTH] Facebook akışı bitmek üzere, popup'ın otomatik kapanması bekleniyor... (Maks 60s)")
            for i in range(60):
                if new_page.is_closed():
                    logger.info("[AUTH] Facebook popup penceresi başarıyla kapandı. Giriş tamamlanıyor.")
                    break
                await asyncio.sleep(1)
            

            
        except Exception as e:
            logger.error(f"[AUTH] Facebook sekmesi içindeki işlemler sırasında hata: {e}")
        finally:
            # İşlemler bitince yeni açılan pencere hâlâ açıksa kapat
            try:
                if not new_page.is_closed():
                    logger.warning("[AUTH] Facebook popup penceresi zaman aşımı nedeniyle zorla kapatılıyor.")
                    await new_page.close()
            except Exception as e_close:
                logger.debug(f"[AUTH] Sekme temizliği sırasında uyarı (önemsiz): {e_close}")
                
            logger.info("[AUTH] Ana sekmeye başarıyla dönüldü.")
            try:
                logger.info("[AUTH] Girişin web sitesine yansıması bekleniyor (Sign In butonunun kaybolması)...")
                # Butonun kaybolmasını 15 saniyeye kadar bekle
                sign_btn = page.locator("button.c-nav-sign").first
                await sign_btn.wait_for(state="hidden", timeout=15000)
                logger.info("[AUTH] Sign In butonu kayboldu, sisteme başarıyla giriş yapıldı!")
                
                # Başarılı olunca güncel cookie'yi kaydet
                try:
                    current_cookies = await page.context.cookies()
                    with open(COOKIES_FILE, "w") as f:
                        json.dump(current_cookies, f)
                    logger.info(f"[AUTH] Yeni cookieler başarıyla '{COOKIES_FILE}' dosyasına kaydedildi.")
                except Exception as e_cook:
                    logger.warning(f"[AUTH] Cookieler kaydedilirken hata oluştu: {e_cook}")
                    
            except Exception:
                logger.warning("[AUTH] Sign In butonu kaybolmadı. Facebook üzerinden giriş başarısız olmuş olabilir.")
                # İstenildiği gibi başarısız durumda eski hazır cookie var ise onu geri yükle
                if os.path.exists(COOKIES_FILE):
                    logger.info("[AUTH] Giriş başarısız olduğundan hazırda olan eski cookieler zorla yükleniyor...")
                    try:
                        with open(COOKIES_FILE, "r") as f:
                            old_cookies = json.load(f)
                        await page.context.add_cookies(old_cookies)
                        await page.reload()
                        logger.info("[AUTH] Eski hazır cookieler geri yüklendi.")
                    except Exception as e_rec:
                        logger.warning(f"[AUTH] Eski cookielerin geri yüklenmesinde hata: {e_rec}")
                
    else:
        logger.info("[AUTH] Facebook butonu bulunamadı/tıklanmadı.")
