-- =====================================================
-- CHẠY FILE NÀY TRONG LOCAL POSTGRESQL ĐỂ TẠO TRIGGERS
-- =====================================================

-- 1. Tạo audit log cho MCC (nếu chưa có)
CREATE TABLE IF NOT EXISTS mcc_audit_log (
    id SERIAL PRIMARY KEY,
    operation VARCHAR(10),
    changed_at TIMESTAMP DEFAULT NOW(),
    data JSONB
);

-- 2. Tạo audit log cho MPASS (nếu chưa có)
CREATE TABLE IF NOT EXISTS mpass_audit_log (
    id SERIAL PRIMARY KEY,
    operation VARCHAR(10),
    changed_at TIMESTAMP DEFAULT NOW(),
    data JSONB
);

-- 3. Function notify cho MCC
DROP FUNCTION IF EXISTS mcc_notify_change() CASCADE;

CREATE OR REPLACE FUNCTION mcc_notify_change()
RETURNS TRIGGER AS $$
DECLARE
    payload JSONB;
BEGIN
    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
        payload = jsonb_build_object(
            'operation', TG_OP,
            'table', 'MCC',
            'timestamp', NOW(),
            'data', row_to_json(NEW)
        );
    ELSIF TG_OP = 'DELETE' THEN
        payload = jsonb_build_object(
            'operation', TG_OP,
            'table', 'MCC',
            'timestamp', NOW(),
            'data', row_to_json(OLD)
        );
    END IF;
    
    INSERT INTO mcc_audit_log (operation, data) VALUES (TG_OP, payload);
    PERFORM pg_notify('mcc_changes', payload::text);
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 4. Function notify cho MPASS
DROP FUNCTION IF EXISTS mpass_notify_change() CASCADE;

CREATE OR REPLACE FUNCTION mpass_notify_change()
RETURNS TRIGGER AS $$
DECLARE
    payload JSONB;
BEGIN
    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
        payload = jsonb_build_object(
            'operation', TG_OP,
            'table', 'MPASS',
            'timestamp', NOW(),
            'data', row_to_json(NEW)
        );
    ELSIF TG_OP = 'DELETE' THEN
        payload = jsonb_build_object(
            'operation', TG_OP,
            'table', 'MPASS',
            'timestamp', NOW(),
            'data', row_to_json(OLD)
        );
    END IF;
    
    INSERT INTO mpass_audit_log (operation, data) VALUES (TG_OP, payload);
    PERFORM pg_notify('mpass_changes', payload::text);
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 5. Tạo trigger cho MCC
DROP TRIGGER IF EXISTS mcc_change_trigger ON mcc;

CREATE TRIGGER mcc_change_trigger
AFTER INSERT OR UPDATE OR DELETE ON mcc
FOR EACH ROW EXECUTE FUNCTION mcc_notify_change();

-- 6. Tạo trigger cho MPASS
DROP TRIGGER IF EXISTS mpass_change_trigger ON mpass;

CREATE TRIGGER mpass_change_trigger
AFTER INSERT OR UPDATE OR DELETE ON mpass
FOR EACH ROW EXECUTE FUNCTION mpass_notify_change();

-- 7. Kiểm tra triggers
SELECT 
    trigger_name, 
    event_object_table, 
    action_timing, 
    event_manipulation
FROM information_schema.triggers
WHERE event_object_table IN ('mcc', 'mpass');