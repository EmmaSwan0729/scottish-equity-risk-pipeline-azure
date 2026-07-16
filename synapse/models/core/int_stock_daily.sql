-- To rerun: first drop the existing external table
-- IF OBJECT_ID('CORE.int_stock_daily', 'U') IS NOT NULL
-- DROP EXTERNAL TABLE CORE.int_stock_daily;

CREATE EXTERNAL TABLE CORE.int_stock_daily
WITH (
    LOCATION = 'int_stock_daily/',
    DATA_SOURCE = CuratedDataSource,
    FILE_FORMAT = ParquetFormat
)
AS
SELECT
    date,
    symbol,
    [close],
    LAG([close]) OVER (PARTITION BY symbol ORDER BY date) AS prev_close,
    ROUND(
        (CAST([close] AS FLOAT) - LAG([close]) OVER (PARTITION BY symbol ORDER BY date))
        / NULLIF(LAG([close]) OVER (PARTITION BY symbol ORDER BY date), 0), 6) AS daily_return,
    [open],
    high,
    low,
    volume
FROM STAGING.stg_stock_prices