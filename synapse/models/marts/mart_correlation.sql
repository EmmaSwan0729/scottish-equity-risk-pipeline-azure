IF OBJECT_ID('MARTS.mart_correlation', 'U') IS NOT NULL
    DROP EXTERNAL TABLE MARTS.mart_correlation;

CREATE EXTERNAL TABLE MARTS.mart_correlation
WITH (
    LOCATION = 'mart_correlation/',
    DATA_SOURCE = CuratedDataSource,
    FILE_FORMAT = ParquetFormat
)
AS
WITH daily_returns AS (
    SELECT symbol, date, daily_return
    FROM CORE.int_stock_daily
    WHERE daily_return IS NOT NULL
),
paired AS (
    SELECT
        a.symbol AS symbol_1,
        b.symbol AS symbol_2,
        CAST(a.daily_return AS FLOAT) AS x,
        CAST(b.daily_return AS FLOAT) AS y
    FROM daily_returns a
    JOIN daily_returns b
        ON a.date = b.date
        AND a.symbol < b.symbol
),
stats AS (
    SELECT
        symbol_1,
        symbol_2,
        COUNT(*) AS n,
        SUM(x) AS sum_x,
        SUM(y) AS sum_y,
        SUM(x * y) AS sum_xy,
        SUM(x * x) AS sum_x2,
        SUM(y * y) AS sum_y2
    FROM paired
    GROUP BY symbol_1, symbol_2
)
SELECT
    symbol_1,
    symbol_2,
    ROUND(
        (n * sum_xy - sum_x * sum_y)
        / NULLIF(SQRT((n * sum_x2 - sum_x * sum_x) * (n * sum_y2 - sum_y * sum_y)), 0)
    , 4) AS correlation
FROM stats
ORDER BY correlation DESC;

SELECT * FROM MARTS.mart_correlation
ORDER BY correlation DESC;