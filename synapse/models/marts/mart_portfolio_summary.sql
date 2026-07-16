IF OBJECT_ID('MARTS.mart_portfolio_summary', 'U') IS NOT NULL
    DROP EXTERNAL TABLE MARTS.mart_portfolio_summary;

CREATE EXTERNAL TABLE MARTS.mart_portfolio_summary
WITH (
    LOCATION = 'mart_portfolio_summary/',
    DATA_SOURCE = CuratedDataSource,
    FILE_FORMAT = ParquetFormat
)
AS
WITH daily_returns AS (
    SELECT * FROM CORE.int_stock_daily
    WHERE daily_return IS NOT NULL
),
latest_prices AS (
    SELECT
        symbol,
        [close] AS latest_close,
        date AS latest_date
    FROM CORE.int_stock_daily
    WHERE date = (SELECT MAX(date) FROM CORE.int_stock_daily)
),
summary AS (
    SELECT
        d.symbol,
        COUNT(*) AS trading_days,
        ROUND(AVG(d.[close]), 4) AS avg_close,
        ROUND(MIN(d.[close]), 4) AS min_close,
        ROUND(MAX(d.[close]), 4) AS max_close,
        ROUND(AVG(d.volume), 0) AS avg_volume,
        l.latest_close,
        l.latest_date
    FROM daily_returns d
    JOIN latest_prices l ON d.symbol = l.symbol
    GROUP BY d.symbol, l.latest_close, l.latest_date
)
SELECT * FROM summary
ORDER BY symbol;

SELECT * FROM MARTS.mart_portfolio_summary
ORDER BY symbol;