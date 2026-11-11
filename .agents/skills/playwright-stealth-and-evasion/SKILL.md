---
name: playwright-stealth-and-evasion
description: >-
  Guides async Playwright browser scraping with persistent contexts,
  playwright-stealth, resource route blocking, global rate-limit cooldown locks,
  single-roundtrip DOM evaluation, ElementHandle disposal, dynamic proxy
  rotation, OAuth/session login recovery, and Docker Xvfb/x11vnc/fluxbox virtual
  displays. Use when writing or modifying `scraper.py`, `proxy_manager.py`,
  `facebook_auth.py`, `Dockerfile`, `docker-compose.yml`, or `start_worker.sh`.
license: MIT
---

# Playwright Stealth, Rate-Limit Cooldown, Proxy Rotation & Docker Xvfb

Use this skill when building or maintaining resilient Playwright scrapers in Python that must run 24/7 without memory leaks, rate-limit bans, or headless browser fingerprint blocks.

## When to Use

- Configuring `async_playwright()` with persistent profiles (`launch_persistent_context`) and `playwright-stealth`.
- Blocking heavy network assets (`image`, `media`, `font`, `stylesheet`) to reduce proxy bandwidth and CPU usage.
- Coordinating global rate limits and toast-triggered cooldowns across multiple concurrent worker tabs.
- Extracting complex row data from dynamic tables/lists without `ElementHandle` memory leaks or IPC bottlenecks.
- Implementing automatic proxy health checks, shuffling, and browser restart loops on proxy/timeout failures.
- Containerizing headful Playwright bots with `Xvfb`, `fluxbox`, and `x11vnc` for remote visual inspection.

---

## 1. Persistent Browser Context, Stealth & Resource Blocking

Always launch a persistent context (`launch_persistent_context`) so cookies, `localStorage`, and OAuth sessions survive restarts:

```python
args = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-popup-blocking",
    "--lang=en-US",
]
context = await playwright.chromium.launch_persistent_context(
    user_data_dir=profile_dir,
    headless=config.HEADLESS,
    viewport={"width": 1920, "height": 1080},
    args=args,
    ignore_https_errors=True,
    proxy={"server": proxy_url} if proxy_url else None,
)
```

### Bandwidth & CPU Optimization via Route Interception
Abort unnecessary media and font requests before they hit the proxy:

```python
block_types = ["image", "media", "font"]
if getattr(config, "BLOCK_CSS", False):
    block_types.append("stylesheet")

async def block_resources(route):
    if route.request.resource_type in block_types:
        await route.abort()
    else:
        await route.continue_()

await context.route("**/*", block_resources)
```

### Apply `playwright-stealth` to the Context
```python
from playwright_stealth import Stealth
await Stealth().apply_stealth_async(context)
```

---

## 2. Shared Rate-Limit Lock & Global Toast Cooldown

When multiple worker tabs share a browser context or IP, independent `asyncio.sleep()` calls are insufficient—workers will fire requests at the exact same millisecond.

### Pattern: Lock-Protected Jitter + Global Cooldown
1. Maintain `self.action_lock = asyncio.Lock()`, `self.last_action_time = 0.0`, and `self.global_cooldown_until = 0.0` on the `DataScraper` instance.
2. Check `self.global_cooldown_until` **inside** `async with self.action_lock:` so when one worker hits a rate-limit toast, all queued workers immediately wait for the cooldown to expire before firing their next navigation or click:

```python
async def wait_for_rate_limit(self, action_name: str = "Action"):
    delay = random.uniform(config.GLOBAL_RATE_LIMIT_MIN, config.GLOBAL_RATE_LIMIT_MAX)
    async with self.action_lock:
        now = time.time()
        if self.global_cooldown_until > now:
            await asyncio.sleep(self.global_cooldown_until - now)
            now = time.time()
        elapsed = now - self.last_action_time
        if elapsed < delay:
            await asyncio.sleep(delay - elapsed)
        self.last_action_time = time.time()
```

3. After every critical navigation or sort/filter click, check for error toast banners (e.g., `.p-toast--error`) with a short timeout (`800ms`) and set `self.global_cooldown_until = time.time() + 15.0` if visible.

---

## 3. Single-Roundtrip DOM Parsing & Memory Leak Prevention

### Anti-Pattern (Slow & Leaky)
Calling `await row.locator(...).inner_text()` 5 separate times per row across 20 rows performs 100+ async IPC roundtrips between Python and Chromium, and leaving `evaluate_handle` references open leaks renderer memory until Chromium crashes.

### Best Practice
1. Extract all fields of a listing row (`seller`, `wearValue`, `isStatTrak`, `listingPrice`, `hasBargain`) in a **single** `await row_handle.evaluate(...)` JavaScript execution returning a plain dict.
2. **ALWAYS** dispose every `JSHandle` / `ElementHandle` once the loop finishes:
   ```python
   for r in rows:
       await r["handle"].dispose()
   ```
3. **ALWAYS** close the worker's page tab inside a `finally:` block in `worker_task`:
   ```python
   finally:
       if worker_page and not worker_page.is_closed():
           await worker_page.close()
       queue.task_done()
   ```

---

## 4. Dynamic Proxy Rotation Loop (`proxy_manager.py` + `main_worker.py`)

When running distributed workers with dynamic proxies:
1. **Shuffle Before Testing**: Always call `random.shuffle(proxies)` in `get_working_proxy()` so multiple containers starting simultaneously do not pick the exact same proxy first.
2. **Fast Liveness Check**: Validate candidate proxies against `https://api.ipify.org?format=json` with `timeout=5` before launching Chromium.
3. **Fail-Fast Rotation**: If a worker encounters a dead proxy or connection failure (`TimeoutError`, `net::ERR_`), close the worker tab, bubble the exception up to the outer proxy loop, remove the failed proxy from the list, close the browser context in `finally:`, and restart with the next working proxy.

---

## 5. Docker + Xvfb + x11vnc + Fluxbox (Headful Container Pattern)

Running `headless=False` inside a lightweight virtual display (`Xvfb`) avoids headless browser telemetry while keeping the bot containerized and observable via VNC:

- **Startup Script (`start_worker.sh`)**:
  1. Always remove stale X11 locks first (`rm -f /tmp/.X*-lock`) so `restart: unless-stopped` never fails after an unclean container stop.
  2. Start `Xvfb :99 -screen 0 1920x1080x24 &` and `export DISPLAY=:99`.
  3. Start `fluxbox &` (prevents Playwright window focus/sizing quirks) and `x11vnc -display :99 -nopw -listen 0.0.0.0 -xkb -ncache 10 -ncache_cr -forever &`.
  4. Map each worker container to a distinct host port (`5901:5900`, `5902:5900`, etc.) in `docker-compose.yml`.

---

## Validation Checklist

- [ ] Every `page.goto()` is wrapped in `asyncio.wait_for(..., timeout=config.PAGE_LOAD_TIMEOUT + 5)` with `wait_until="domcontentloaded"`.
- [ ] Every `evaluate_handle()` object is explicitly `.dispose()`d after use.
- [ ] Every worker page (`context.new_page()`) is closed in a `finally:` block and `queue.task_done()` is called exactly once per `queue.get()`.
- [ ] Rate-limit sleeps use randomized jitter (`random.uniform(min, max)`) synchronized via `self.action_lock`.
- [ ] `start_worker.sh` cleans `/tmp/.X*-lock` before launching `Xvfb`.
