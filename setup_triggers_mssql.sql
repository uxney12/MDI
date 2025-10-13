-- =====================================================
-- CHẠY FILE NÀY TRONG LOCAL MS SQL SERVER ĐỂ TẠO TRIGGERS
-- Database: Mpass
-- Table: mpass (Microsoft SQL Server)
-- Mục tiêu: Ghi lại audit log và gửi notification tương tự PostgreSQL (mcc)
-- =====================================================

USE Mpass;  
GO

IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='mpass_audit_log' AND xtype='U')
BEGIN
    CREATE TABLE dbo.mpass_audit_log (
        id INT IDENTITY(1,1) PRIMARY KEY,
        operation VARCHAR(10) NOT NULL,
        changed_at DATETIME2 DEFAULT SYSUTCDATETIME(),
        data NVARCHAR(MAX) NOT NULL
    );
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='idx_mpass_audit_changed_at')
    CREATE INDEX idx_mpass_audit_changed_at ON dbo.mpass_audit_log(changed_at);
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='idx_mpass_audit_operation')
    CREATE INDEX idx_mpass_audit_operation ON dbo.mpass_audit_log(operation);
GO

DECLARE @is_broker_enabled INT;
SELECT @is_broker_enabled = is_broker_enabled FROM sys.databases WHERE name = 'Mpass';

IF @is_broker_enabled = 0
BEGIN
    ALTER DATABASE Mpass SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE;
    PRINT 'Service Broker đã được bật cho Mpass.';
END
GO

IF OBJECT_ID('dbo.trg_mpass_changes', 'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_mpass_changes;
GO

CREATE TRIGGER dbo.trg_mpass_changes
ON dbo.mpass
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @operation VARCHAR(10);
    DECLARE @timestamp DATETIME2 = SYSUTCDATETIME();
    DECLARE @data NVARCHAR(MAX);

    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
        SET @operation = 'UPDATE';
    ELSE IF EXISTS (SELECT * FROM inserted)
        SET @operation = 'INSERT';
    ELSE
        SET @operation = 'DELETE';

    IF @operation = 'DELETE'
        SELECT @data = (
            SELECT 
                @operation AS operation,
                'mpass' AS [table],
                'Mpass' AS [database],
                'mssql' AS source,
                @timestamp AS [timestamp],
                (SELECT * FROM deleted FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) AS data
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        );
    ELSE
        SELECT @data = (
            SELECT 
                @operation AS operation,
                'mpass' AS [table],
                'Mpass' AS [database],
                'mssql' AS source,
                @timestamp AS [timestamp],
                (SELECT * FROM inserted FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) AS data
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        );

    INSERT INTO dbo.mpass_audit_log (operation, changed_at, data)
    VALUES (@operation, @timestamp, @data);
END
GO

SELECT 
    t.name AS trigger_name,
    OBJECT_NAME(t.parent_id) AS table_name,
    t.type_desc,
    t.create_date,
    CASE 
        WHEN OBJECTPROPERTY(t.object_id, 'ExecIsInsertTrigger') = 1 THEN 'INSERT '
        ELSE ''
    END +
    CASE 
        WHEN OBJECTPROPERTY(t.object_id, 'ExecIsUpdateTrigger') = 1 THEN 'UPDATE '
        ELSE ''
    END +
    CASE 
        WHEN OBJECTPROPERTY(t.object_id, 'ExecIsDeleteTrigger') = 1 THEN 'DELETE'
        ELSE ''
    END AS events
FROM sys.triggers t
WHERE OBJECT_NAME(t.parent_id) = 'mpass';
GO


