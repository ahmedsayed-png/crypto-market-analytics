-- ==============================================================================
-- PostgreSQL Data Warehouse Initialization DDL Script
-- Target DB: crypto_dw
-- ==============================================================================

-- 1. Dimension Table: Cryptocurrency Trading Symbols
CREATE TABLE IF NOT EXISTS dim_symbol (
    symbol          VARCHAR(20) PRIMARY KEY,
    base_asset      VARCHAR(10) NOT NULL,
    quote_asset     VARCHAR(10) NOT NULL,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Initial seed data for target trading pairs
INSERT INTO dim_symbol (symbol, base_asset, quote_asset) VALUES
('BTCUSDT', 'BTC', 'USDT'),
('ETHUSDT', 'ETH', 'USDT'),
('BNBUSDT', 'BNB', 'USDT'),
('SOLUSDT', 'SOL', 'USDT'),
('XRPUSDT', 'XRP', 'USDT'),
('ADAUSDT', 'ADA', 'USDT'),
('DOGEUSDT', 'DOGE', 'USDT'),
('AVAXUSDT', 'AVAX', 'USDT'),
('DOTUSDT', 'DOT', 'USDT'),
('LINKUSDT', 'LINK', 'USDT')
ON CONFLICT (symbol) DO NOTHING;

-- 2. Fact Table: 1-Minute Candlestick (Kline) Metrics
CREATE TABLE IF NOT EXISTS fact_klines_min (
    open_time               TIMESTAMP NOT NULL,
    symbol                  VARCHAR(20) NOT NULL,
    open                    NUMERIC(18, 8) NOT NULL,
    high                    NUMERIC(18, 8) NOT NULL,
    low                     NUMERIC(18, 8) NOT NULL,
    close                   NUMERIC(18, 8) NOT NULL,
    volume                  NUMERIC(24, 8) NOT NULL,
    quote_volume            NUMERIC(24, 8) NOT NULL,
    number_of_trades        BIGINT NOT NULL,
    taker_buy_volume        NUMERIC(24, 8) NOT NULL,
    taker_buy_quote_volume  NUMERIC(24, 8) NOT NULL,
    processed_at            TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_klines_min_time_symbol ON fact_klines_min (open_time DESC, symbol);
CREATE INDEX IF NOT EXISTS idx_klines_min_symbol ON fact_klines_min (symbol);

-- 3. Fact Table: Daily Aggregated Market Metrics
CREATE TABLE IF NOT EXISTS fact_klines_daily (
    kline_date              DATE NOT NULL,
    symbol                  VARCHAR(20) NOT NULL,
    open_price              NUMERIC(18, 8) NOT NULL,
    high_price              NUMERIC(18, 8) NOT NULL,
    low_price               NUMERIC(18, 8) NOT NULL,
    close_price             NUMERIC(18, 8) NOT NULL,
    total_volume            NUMERIC(24, 8) NOT NULL,
    total_quote_volume      NUMERIC(24, 8) NOT NULL,
    total_trades            BIGINT NOT NULL,
    price_change_pct        NUMERIC(10, 4),
    updated_at              TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (kline_date, symbol)
);

-- 4. Fact Table: Spark MLlib Model Inference Output
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id           BIGSERIAL PRIMARY KEY,
    open_time               TIMESTAMP NOT NULL,
    symbol                  VARCHAR(20) NOT NULL,
    current_close           NUMERIC(18, 8) NOT NULL,
    predicted_direction     VARCHAR(10) NOT NULL, -- 'UP', 'DOWN', 'FLAT'
    prediction_probability  NUMERIC(6, 4) NOT NULL,
    model_version           VARCHAR(32) NOT NULL,
    created_at              TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_predictions_symbol_time ON fact_predictions (symbol, open_time DESC);

-- ------------------------------------------------------------------------------
-- Analytical Views for Apache Superset Dashboards
-- ------------------------------------------------------------------------------

-- Market Overview Summary View
CREATE OR REPLACE VIEW vw_market_overview AS
SELECT 
    symbol,
    COUNT(*) AS min_bars_count,
    MIN(open_time) AS earliest_record,
    MAX(open_time) AS latest_record,
    AVG(close) AS avg_close_price,
    SUM(volume) AS total_volume
FROM fact_klines_min
GROUP BY symbol;

-- Dynamic Coin Analysis View
CREATE OR REPLACE VIEW vw_coin_analysis AS
SELECT 
    f.open_time,
    f.symbol,
    f.open,
    f.high,
    f.low,
    f.close,
    f.volume,
    f.quote_volume,
    f.number_of_trades,
    (f.close - f.open) AS price_change,
    ROUND(((f.close - f.open) / NULLIF(f.open, 0)) * 100, 4) AS price_change_pct
FROM fact_klines_min f;
