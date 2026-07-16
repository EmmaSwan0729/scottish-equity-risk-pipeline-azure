CREATE OR ALTER VIEW STAGING.stg_stock_prices AS
SELECT
    CAST(DATEADD(SECOND, date / 1000000000, '1970-01-01') AS DATE) AS date,
    symbol,
    [open],
    high,
    low,
    [close],
    volume,
    ingested_at
FROM OPENROWSET(
    BULK 'https://stscotequityriskuk.dfs.core.windows.net/raw/stock_prices/*/*.parquet',
    FORMAT = 'PARQUET'
) AS raw_data