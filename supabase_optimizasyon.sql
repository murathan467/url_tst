-- ============================================================================
-- SUPABASE (BuffBotDb) VERİTABANI KİLİTLENME (LOCK) VE ŞİŞME (BLOAT) ÇÖZÜMÜ
-- ============================================================================

-- 1. İNDEKS VE BELLEK (RAM / SWAP) OPTİMİZASYONU
-- PRIMARY KEY (id) ve UNIQUE (skin_id, key_name) zaten otomatik B-Tree indeks oluşturur.
-- Kopya indeksleri silerek Supabase'in 411 MB RAM'indeki "Cache + Buffers" yükünü azaltıyoruz.
DROP INDEX IF EXISTS public.idx_skins_id;
DROP INDEX IF EXISTS public.idx_skins_skin_id;
DROP INDEX IF EXISTS public.idx_skins_processed_at;
DROP INDEX IF EXISTS public.idx_system_state_key;

CREATE INDEX IF NOT EXISTS idx_notified_bargains_lookup ON public.notified_bargains (skin_name, seller, float_value);

-- 2. KİLİTLENMEYİ ÖNLEYEN RPC FONKSİYONU (FOR UPDATE SKIP LOCKED — EN GÜNCEL ÖNCELİKLİ)
-- 4 farklı bot düğümü aynı anda iş çekerken birbirlerinin kilitlediği satırları beklemez (SKIP LOCKED),
-- anında en güncel (ORDER BY id DESC) boş satırları alıp siler.
CREATE OR REPLACE FUNCTION public.get_and_delete_skins(batch_limit integer DEFAULT 8)
RETURNS SETOF public.skins
LANGUAGE plpgsql
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

-- 3. 150 SATIRI AŞINCA EN ESKİ 100 SATIRI SİLEN HAFİF RPC FONKSİYONU (COUNT(*) TARAMASI YOK)
CREATE OR REPLACE FUNCTION public.trim_old_skins(max_rows integer DEFAULT 150, delete_count integer DEFAULT 100)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    deleted_rows integer := 0;
BEGIN
    -- COUNT(*) tüm tabloyu tarayıp RAM/IO tükettiği için sadece max_rows'uncu satır var mı diye bakıyoruz
    IF EXISTS (SELECT 1 FROM public.skins OFFSET max_rows LIMIT 1) THEN
        WITH to_delete AS (
            SELECT id FROM public.skins
            ORDER BY id ASC
            LIMIT delete_count
            FOR UPDATE SKIP LOCKED
        )
        DELETE FROM public.skins
        WHERE id IN (SELECT id FROM to_delete);

        GET DIAGNOSTICS deleted_rows = ROW_COUNT;
    END IF;

    -- Bildirim tablosunun da RAM'de şişmesini önle (En son 300 bildirimi tut)
    IF EXISTS (SELECT 1 FROM public.notified_bargains OFFSET 300 LIMIT 1) THEN
        DELETE FROM public.notified_bargains
        WHERE id IN (
            SELECT id FROM public.notified_bargains
            ORDER BY id DESC
            OFFSET 300
        );
    END IF;

    RETURN deleted_rows;
END;
$$;

-- 4. AUTOVACUUM AYARLARI (Silinen 457.000+ satırın diskte "Dead Tuple" olarak şişmesini önler)
ALTER TABLE public.skins SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 50,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_analyze_threshold = 50
);

ALTER TABLE public.system_state SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_vacuum_threshold = 50
);

-- 5. POSTGREST ŞEMA ÖNBELLEĞİNİ YENİLE
NOTIFY pgrst, 'reload schema';
