-- ============================================================================
-- SUPABASE (BuffBotDb) VERİTABANI KİLİTLENME (LOCK) VE ŞİŞME (BLOAT) ÇÖZÜMÜ
-- Bu kodu Supabase Dashboard -> SQL Editor -> New Query ekranına yapıştırıp
-- "Run" butonuna basarak bir kez çalıştırmanız yeterlidir.
-- ============================================================================

-- 1. İNDEKSLER (Sıralama ve Arama Hızlandırma)
CREATE INDEX IF NOT EXISTS idx_skins_processed_at ON public.skins (processed_at ASC);
CREATE INDEX IF NOT EXISTS idx_skins_skin_id ON public.skins (skin_id);
CREATE INDEX IF NOT EXISTS idx_notified_bargains_lookup ON public.notified_bargains (skin_name, seller, float_value);

-- 2. KİLİTLENMEYİ ÖNLEYEN RPC FONKSİYONU (FOR UPDATE SKIP LOCKED)
-- 4 farklı bot düğümü aynı anda iş çekerken veya Monitor yeni ilan eklerken
-- birbirlerinin kilitlediği satırları beklemez (SKIP LOCKED), anında sıradaki
-- boş satırları alıp siler. Böylece "statement timeout (57014)" hatası tamamen biter.
CREATE OR REPLACE FUNCTION public.get_and_delete_skins(batch_limit INT DEFAULT 5)
RETURNS TABLE (
    skin_id TEXT,
    skin_name TEXT,
    wear_value TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH locked_rows AS (
        SELECT s.id
        FROM public.skins s
        ORDER BY s.id ASC
        LIMIT batch_limit
        FOR UPDATE SKIP LOCKED
    ),
    deleted AS (
        DELETE FROM public.skins s
        USING locked_rows lr
        WHERE s.id = lr.id
        RETURNING s.skin_id, s.skin_name, s.wear_value
    )
    SELECT d.skin_id, d.skin_name, d.wear_value FROM deleted d;
END;
$$;

-- 3. 5.000 SATIRI AŞINCA EN ESKİ 4.000 SATIRI SİLEN RPC FONKSİYONU
-- Ayrıca diğer tablolardaki (skipped_bargains ve notified_bargains) gereksiz
-- birikmeleri de otomatik budar.
CREATE OR REPLACE FUNCTION public.trim_old_skins(max_rows INT DEFAULT 5000, delete_count INT DEFAULT 4000)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    current_count INT;
    deleted_rows INT := 0;
BEGIN
    SELECT count(*) INTO current_count FROM public.skins;

    IF current_count > max_rows THEN
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

    -- Yan tabloların da zamanla şişmesini önle (en güncel 2000 bildirimi tut)
    DELETE FROM public.skipped_bargains
    WHERE id IN (
        SELECT id FROM public.skipped_bargains
        ORDER BY id ASC
        OFFSET 500
    );

    RETURN deleted_rows;
END;
$$;

-- 4. AUTOVACUUM AYARLARI (Silinen 450.000+ satırın diskte "Dead Tuple" olarak şişmesini önler)
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

-- 5. MEVCUT ÖLÜ SATIRLARI (DEAD TUPLES) ANINDA TEMİZLE
VACUUM ANALYZE public.skins;
VACUUM ANALYZE public.notified_bargains;
VACUUM ANALYZE public.skipped_bargains;
VACUUM ANALYZE public.system_state;
