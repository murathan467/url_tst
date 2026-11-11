# AGENTS.md — Python Scraping & Distributed Bot Orchestration Rules
**(Graphify + Addy Skills + Ponytail + Playwright/Stealth/Supabase Scraping Skills)**

This repository uses a 4-phase agent architecture tailored for **24/7 Asynchronous Python Web Scraping & Distributed Worker Bots** (`Playwright`, `playwright-stealth`, `asyncio`, `Supabase/PostgreSQL`, `Docker/Xvfb/x11vnc`, `Telegram Bot API`). Follow the strict hierarchy and conflict-resolution rules below for every task.

---

## 1. System Hierarchy & Phase Routing

1. **Discovery & Context (ALWAYS FIRST):** Governed by **`graphify`**
2. **Specification & Planning:** Governed by **`addyosmani/agent-skills`** + **`web-scraping`**
3. **Code Implementation (Diff Discipline + Bot Resilience Standards):** Governed by **`ponytail` (`full` mode)** + **`Scraping / Playwright / Supabase Skills`**
4. **Verification, Testing & Resilience Audit:** Governed by **`addyosmani/agent-skills`** + **`ponytail-review`** + **`scraper-resilience-auditor`**

---

## 2. Phase 1: Context & Codebase Discovery (`graphify`)

Before using raw `grep`, `find`, or reading multiple `.py` / `.sql` files sequentially:

- **Check for Graph:** If `graphify-out/graph.json` exists, ALWAYS query the knowledge graph first:
  - Concept & relationship lookup: `graphify query "<question>"`
  - Symbol connections & callers: `graphify explain "<ClassOrFunctionName>"` (e.g., `DataScraper`, `get_work_batch`, `save_skins_batch`)
  - Dependency/call path between two modules: `graphify path "monitor_task" "save_skins_batch"`
- **Read Only What Matches:** Open only the exact files and line ranges returned by `graphify`. Never read `GRAPH_REPORT.md` or `graph.json` in full unless performing a broad architectural audit.
- **Stale Graph Guard:** After creating or modifying `.py` files, classes, or functions, run `graphify update .` before relying on graph queries in subsequent steps.

---

## 3. Phase 2: Spec, Planning & Scraping Skill Routing

Always invoke lifecycle and scraping stack skills explicitly by their skill name:

- **Underspecified bot requirements / target site rules** -> Activate `interview-me` or `spec-driven-development`
- **Multi-file bot feature or worker scaling plan** -> Activate `planning-and-task-breakdown`
- **Monitor + Dispatcher + Worker topology, `asyncio.to_thread`, RAM caches & Telegram alerts** -> Activate `distributed-scraper-architecture`
- **Playwright persistent context, `playwright-stealth`, route blocking, `action_lock` cooldowns, DOM `evaluate`, proxy rotation & Docker `Xvfb`/`x11vnc`** -> Activate `playwright-stealth-and-evasion` + `playwright-py-skill`
- **Supabase / PostgreSQL queue RPCs (`FOR UPDATE SKIP LOCKED`), bulk upserts (`returning="minimal"`), `autovacuum` & index tuning** -> Activate `supabase-postgres-best-practices` + `distributed-scraper-architecture`
- **Long-running scrape checkpoints, parallel pipelines & rate-limit recovery** -> Activate `web-scraping`
- **Anti-bot escalation, adaptive selectors & JS-heavy extraction** -> Activate `scrapling-official` or `crawl4ai`
- **Live DOM inspection & network waterfall debugging** -> Activate `browser-testing-with-devtools`

---

## 4. Phase 3: Implementation & Conflict Resolution (`ponytail` vs `Bot Resilience`)

During code generation, apply **`ponytail` (`full` level)** rules with the following **mandatory Scraping Bot overrides**:

### Rule A — Monitor / Dispatcher / Worker Separation Overrides Ponytail Collapsing
- **Conflict:** Ponytail instructs to merge single-use functions or collapse separate modules into one script.
- **Resolution:** **NEVER** collapse `monitor_task` (producer), `dispatcher_task` (queue puller with adaptive backoff), and `worker_task` (consumer) into a single monolithic loop, and **NEVER** merge `main_monitor.py`, `main_worker.py`, `main_local.py`, and `main_vm.py`. They are deployed independently across host machines, VMs, and Docker containers. Apply Ponytail's minimalism *inside* each function.

### Rule B — Non-Blocking `asyncio` & Supabase Disk IO Discipline (`distributed-scraper-architecture` + `supabase-postgres-best-practices`)
- **NEVER** call synchronous `supabase-py` (`database.py`) or `requests` (`telegram_bot.py`, `proxy_manager.py`) directly inside an `async def` coroutine—always wrap them in `await asyncio.to_thread(...)`.
- **NEVER** delete RAM caches (`_known_skin_ids_cache`, `_notified_cache`, `_last_scanned_id_cache`), `httpx.Client` timeout/HTTP1.1 patches, `ignore_duplicates=True`, `returning="minimal"`, or PostgreSQL `FOR UPDATE SKIP LOCKED` RPCs in the name of "simplifying code." Removing these causes instant Supabase Disk IO exhaustion and `57014 statement timeout` deadlocks.

### Rule C — Rate-Limit Cooldown & Memory Leak Guards (`playwright-stealth-and-evasion`)
- **NEVER** remove `self.action_lock`, `wait_for_rate_limit()`, or `check_and_handle_toast()` (`global_cooldown_until`).
- **ALWAYS** extract multi-field listing rows in a single `row_handle.evaluate(...)` call, explicitly `await handle.dispose()` every `ElementHandle` / `JSHandle`, and close every `worker_page` inside a `finally:` block with `queue.task_done()`.

### Rule D — Reuse via Graphify (No Duplicate Parsers/DB Helpers)
- Before writing any new URL parser, price/float regex extractor, DB retry wrapper, or Telegram formatter, run `graphify query` to check if an equivalent already exists in `scraper.py`, `database.py`, or `telegram_bot.py`.

---

## 5. Phase 4: Verification & Pre-Merge Gates

Before marking any multi-file task complete:

1. Run `git diff --stat` and verify adherence to `incremental-implementation` (small, verifiable slice, zero unrelated formatting changes).
2. Run syntax/lint and unit checks (`python3 -m py_compile *.py` and `pytest` if tests exist; activate `debugging-and-error-recovery` if anything fails).
3. For `.env` secrets, OAuth login flows (`facebook_auth.py`), proxy handling, and Telegram Markdown escaping, enforce `security-and-hardening` checks.
4. Execute `scraper-resilience-auditor`, `code-review-and-quality`, and `ponytail-review` self-checks before final output.
