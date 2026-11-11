---
name: distributed-scraper-architecture
description: >-
  Architects and optimizes distributed Python web scraping bots using the
  Monitor + Dispatcher + Worker pattern, Supabase/PostgreSQL atomic queues
  (FOR UPDATE SKIP LOCKED), RAM caching, Disk IO protection, non-blocking
  asyncio.to_thread database calls, and Telegram alerting. Use when building,
  refactoring, or scaling multi-worker scraping bots (`main_monitor.py`,
  `main_worker.py`, `main_local.py`, `main_vm.py`, `database.py`, `telegram_bot.py`).
license: MIT
---

# Distributed Scraper Architecture (Monitor + Dispatcher + Worker + Supabase IO)

Use this skill when designing, writing, or debugging high-throughput, 24/7 distributed scraping bots in Python that coordinate work across local machines, VMs, and Docker containers via Supabase/PostgreSQL.

## When to Use

- Structuring a new scraping bot with **Monitor** (producer), **Dispatcher** (queue puller), and **Worker** (consumer) roles.
- Preventing PostgreSQL lock contention (`57014 statement timeout`) when multiple workers pull tasks concurrently.
- Optimizing Supabase Disk IO, connection stability (`httpx` HTTP/1.1 timeouts), RAM caching, and automatic table trimming (`trim_old_skins`).
- Fixing `asyncio` event-loop freezes caused by synchronous `supabase-py` or `requests` calls.
- Sending formatted, properly escaped Telegram notifications (`telegram_bot.py`).

---

## 1. Role Separation & Execution Topologies

Never mix listing discovery, queue polling, and detail-page inspection into a single monolithic loop. Always maintain three composable async tasks that can run together or on separate nodes:

1. **Monitor (`monitor_task` / `main_monitor.py`)**:
   - Scans the main listing/category page at randomized intervals (`SCAN_INTERVAL_MIN` to `SCAN_INTERVAL_MAX`).
   - Tracks `last_scanned_id` in RAM and syncs to `system_state` at most once every 60 seconds (throttling DB writes).
   - Filters out blocked keywords (`BLOCKED_ITEM_KEYWORDS`) and already-known IDs via an in-memory set (`_known_skin_ids_cache`).
   - Pushes newly discovered items in 50-row chunks via Bulk Upsert (`ignore_duplicates=True, returning="minimal"`).
   - Every $N$ cycles (e.g., `cycle_count % 10 == 0`), invokes `trim_old_skins(5000, 4000)` to cap table size.

2. **Dispatcher (`dispatcher_task`)**:
   - Bridges the remote Supabase queue (`skins` table) and the local `asyncio.Queue`.
   - When `queue.qsize()` drops below threshold, pulls a batch atomically via RPC `get_and_delete_skins(batch_limit)`.
   - **Adaptive Backoff**: Uses a short interval (3–4s) when work is found, and gradually increases sleep up to 10–12s when the queue is empty to protect Disk IO.
   - In hybrid local mode (`main_local.py`), waits on `new_work_event.wait()` with a timeout so it wakes up immediately when the Monitor inserts new rows.

3. **Workers (`worker_task` / `main_worker.py` / `main_vm.py`)**:
   - Pull batches from `asyncio.Queue`, open an isolated Playwright tab (`await scraper.context.new_page()`), inspect the detail page, and always close the tab in a `finally:` block.
   - Deduplicate notifications against `_notified_cache` + `notified_bargains` table before sending Telegram alerts.

---

## 2. Atomic Work Queue in PostgreSQL (`FOR UPDATE SKIP LOCKED`)

Never use `SELECT` followed by `DELETE` from Python to claim tasks—multiple workers will grab the same items or trigger row-lock timeouts. Always use an atomic PostgreSQL RPC function with `FOR UPDATE SKIP LOCKED`:

```sql
CREATE OR REPLACE FUNCTION public.get_and_delete_skins(batch_limit integer DEFAULT 5)
RETURNS SETOF public.skins
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN QUERY
  WITH locked_rows AS (
    SELECT s.id
    FROM public.skins s
    ORDER BY s.id DESC
    LIMIT batch_limit
    FOR UPDATE SKIP LOCKED
  )
  DELETE FROM public.skins s
  USING locked_rows lr
  WHERE s.id = lr.id
  RETURNING s.*;
END;
$$;
```

### High-Churn Table Autovacuum Tuning
Because queue tables experience thousands of `INSERT` and `DELETE` operations per hour, dead tuples accumulate rapidly. Always configure aggressive `autovacuum` thresholds on queue and state tables:

```sql
ALTER TABLE public.skins SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 50,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_analyze_threshold = 50
);
```

---

## 3. Supabase Client Hardening & Disk IO Optimization (`database.py`)

### Rule A — Force HTTP/1.1 and Strict Timeouts on `httpx`
The default `httpx` client inside `supabase-py` can hang indefinitely on stale HTTP/2 connections. Patch `httpx.Client.__init__` before creating the Supabase client:

```python
import httpx

_original_client_init = httpx.Client.__init__

def _patched_client_init(self, *args, **kwargs):
    kwargs["http2"] = False
    kwargs["timeout"] = httpx.Timeout(10.0, connect=5.0, read=10.0, write=10.0, pool=5.0)
    _original_client_init(self, *args, **kwargs)

httpx.Client.__init__ = _patched_client_init
```

### Rule B — Reuse Singleton Client with Non-Blocking Backoff
- Never recreate the Supabase client on every query or error (exhausts SSL/TCP sockets). Use a thread-safe singleton (`_client_lock = threading.Lock()`).
- On recoverable network/timeout errors (`57014`, `timed out`, `RemoteProtocolError`, `502`/`503`/`504`), retry with progressive backoff (`10.0s`, `25.0s`) without holding a global lock across threads.

### Rule C — Never Block the `asyncio` Event Loop
`supabase-py` and `requests` are synchronous. Calling them directly inside `async def` freezes all Playwright workers and triggers false page timeouts.
- **ALWAYS** wrap synchronous DB and HTTP calls in `await asyncio.to_thread(...)`:
  ```python
  saved_ok = await asyncio.to_thread(save_skins_batch, batch_data)
  ```

### Rule D — Protect Disk IO Budget (RAM Cache + Minimal Returning)
1. **RAM Caches (`threading.RLock`)**:
   - `_known_skin_ids_cache`: Avoids querying `skins` for already-queued IDs. Clear when exceeding 5,000 items or after table trim.
   - `_notified_cache`: Preload the last 300 entries from `notified_bargains` at startup (`_preload_notified_cache()`) so `is_bargain_notified()` checks RAM in $O(1)$ before hitting the database.
   - `_last_scanned_id_cache`: Serve reads from RAM; throttle DB `upsert` writes to `system_state` to once every 60 seconds.
2. **Zero-Overhead Writes**:
   - Always pass `ignore_duplicates=True` and `returning="minimal"` on `upsert` and `delete` calls so PostgreSQL does not serialize and transmit inserted rows back over the network.
   - Log skipped/duplicate items to local debug logs (`logger.debug`) rather than inserting into a database table on every skip.

---

## 4. Telegram Alerting & Markdown Safety (`telegram_bot.py`)

- Always escape all Markdown v1 special characters in dynamic scraped strings (item names, seller usernames, float values) before sending to Telegram, otherwise a single `_` or `*` in a seller's username will cause `400 Bad Request: can't parse entities`:
  ```python
  def _escape_markdown(text: str) -> str:
      special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
      for char in special_chars:
          text = text.replace(char, f"\\{char}")
      return text
  ```
- Always set `timeout=10` on `requests.post(TELEGRAM_API_URL, ...)` and invoke notification functions via `await asyncio.to_thread(send_notification, ...)` from async workers.

---

## Validation Checklist

- [ ] Queue claims use PostgreSQL `FOR UPDATE SKIP LOCKED` via RPC (`get_and_delete_skins`).
- [ ] All synchronous `database.py` and `telegram_bot.py` calls inside `async def` functions are wrapped in `await asyncio.to_thread(...)`.
- [ ] Bulk inserts use 50-row chunks with `ignore_duplicates=True` and `returning="minimal"`.
- [ ] `httpx.Client` has `http2=False` and explicit 10-second timeouts configured.
- [ ] High-churn tables have `trim_old_skins` periodic cleanup and aggressive `autovacuum` settings.
- [ ] All secrets (`SUPABASE_URL`, `SUPABASE_KEY`, `TELEGRAM_TOKEN`, credentials) are loaded from environment variables / `.env`, never hardcoded.
