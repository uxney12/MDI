-- =====================================================
-- CHẠY FILE NÀY TRONG LOCAL POSTGRESQL ĐỂ TẠO TRIGGERS
-- Database: MDI
-- Table: mcc (PostgreSQL)
-- =====================================================

-- Kết nối vào database MDI
-- psql -h localhost -U postgres -d MDI -f setup_triggers_postgres.sql

CREATE TABLE IF NOT EXISTS mcc_audit_log (
    id SERIAL PRIMARY KEY,
    operation VARCHAR(10),
    changed_at TIMESTAMP DEFAULT NOW(),
    data JSONB
);

CREATE INDEX IF NOT EXISTS idx_mcc_audit_changed_at ON mcc_audit_log(changed_at);
CREATE INDEX IF NOT EXISTS idx_mcc_audit_operation ON mcc_audit_log(operation);

DROP FUNCTION IF EXISTS mcc_notify_change() CASCADE;

CREATE OR REPLACE FUNCTION mcc_notify_change()
RETURNS TRIGGER AS $$
DECLARE
    payload JSONB;
BEGIN
    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
        payload = jsonb_build_object(
            'operation', TG_OP,
            'table', 'mcc',
            'database', 'MDI',
            'source', 'postgresql',
            'timestamp', NOW(),
            'data', row_to_json(NEW)
        );
    ELSIF TG_OP = 'DELETE' THEN
        payload = jsonb_build_object(
            'operation', TG_OP,
            'table', 'mcc',
            'database', 'MDI',
            'source', 'postgresql',
            'timestamp', NOW(),
            'data', row_to_json(OLD)
        );
    END IF;
    
    INSERT INTO mcc_audit_log (operation, data) VALUES (TG_OP, payload);
    
    PERFORM pg_notify('mcc_changes', payload::text);
    
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS mcc_change_trigger ON mcc;

CREATE TRIGGER mcc_change_trigger
AFTER INSERT OR UPDATE OR DELETE ON mcc
FOR EACH ROW EXECUTE FUNCTION mcc_notify_change();

SELECT 
    trigger_name, 
    event_object_table, 
    action_timing, 
    event_manipulation,
    action_statement
FROM information_schema.triggers
WHERE event_object_table = 'mcc';
