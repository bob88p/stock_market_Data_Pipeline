-- ===-- ============================================================
-- 0. DATABASE
-- ============================================================
CREATE DATABASE Stock_db;
GO

USE Stock_db;
GO


-- ============================================================
-- 1. SCHEMAS
-- ============================================================
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stage')
    EXEC('CREATE SCHEMA stage');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO


-- ============================================================
-- 2. STAGE TABLE
-- ============================================================
IF OBJECT_ID('stage.stock_stage', 'U') IS NOT NULL
    DROP TABLE stage.stock_stage;
GO

CREATE TABLE stage.stock_stage (
    trade_date      DATE            NOT NULL,
    ticker          NVARCHAR(10)    NOT NULL,

    open_price      DECIMAL(18, 4)  NOT NULL,
    high_price      DECIMAL(18, 4)  NOT NULL,
    low_price       DECIMAL(18, 4)  NOT NULL,
    close_price     DECIMAL(18, 4)  NOT NULL,
    volume          BIGINT          NOT NULL,

    CONSTRAINT PK_stage_stock PRIMARY KEY (trade_date, ticker)
);
GO


-- ============================================================
-- 3. BRONZE (RAW LAYER)
-- ============================================================
IF OBJECT_ID('bronze.stock_prices_raw', 'U') IS NOT NULL
    DROP TABLE bronze.stock_prices_raw;
GO

CREATE TABLE bronze.stock_prices_raw (
    trade_date      DATE            NOT NULL,
    ticker          NVARCHAR(10)    NOT NULL,

    open_price      DECIMAL(18, 4)  NOT NULL,
    high_price      DECIMAL(18, 4)  NOT NULL,
    low_price       DECIMAL(18, 4)  NOT NULL,
    close_price     DECIMAL(18, 4)  NOT NULL,
    volume          BIGINT          NOT NULL,

    CONSTRAINT PK_bronze_stock PRIMARY KEY (trade_date, ticker)
);
GO


-- ============================================================
-- 4. SILVER (CLEAN + FEATURES)
-- ============================================================
IF OBJECT_ID('silver.stock_prices_clean', 'U') IS NOT NULL
    DROP TABLE silver.stock_prices_clean;
GO

CREATE TABLE silver.stock_prices_clean (
    trade_date        DATE            NOT NULL,
    ticker            NVARCHAR(10)    NOT NULL,

    open_price        DECIMAL(18, 4)  NOT NULL,
    high_price        DECIMAL(18, 4)  NOT NULL,
    low_price         DECIMAL(18, 4)  NOT NULL,
    close_price       DECIMAL(18, 4)  NOT NULL,
    volume            BIGINT          NOT NULL,

    daily_return_pct  DECIMAL(10, 4)  NULL,
    price_range       DECIMAL(18, 4)  NULL,
    ma_7              DECIMAL(18, 4)  NULL,
    volatility_7d     DECIMAL(18, 4)  NULL,

    CONSTRAINT PK_silver_stock PRIMARY KEY (trade_date, ticker)
);
GO


-- ============================================================
-- 5. GOLD (BUSINESS VIEW)
-- ============================================================
CREATE VIEW gold.fact_stock_prices_daily AS
SELECT
    trade_date,
    ticker,
    open_price,
    close_price,
    daily_return_pct,
    price_range,
    ma_7,
    volatility_7d
FROM silver.stock_prices_clean;
GO


-- ============================================================
-- 6. VALIDATION
-- ============================================================
SELECT
    s.name        AS schema_name,
    t.name        AS table_name,
    t.create_date,
    p.rows        AS row_count
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.partitions p ON t.object_id = p.object_id
WHERE s.name IN ('stage', 'bronze', 'silver')
  AND p.index_id IN (0,1)
ORDER BY s.name, t.name;
GO