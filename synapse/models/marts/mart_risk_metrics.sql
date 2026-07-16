CREATE SCHEMA MARTS;

IF OBJECT_ID('MARTS.mart_risk_metrics', 'U') IS NOT NULL
    DROP EXTERNAL TABLE MARTS.mart_risk_metrics;

CREATE EXTERNAL TABLE MARTS.mart_risk_metrics
WITH (
    LOCATION = 'mart_risk_metrics/',
    DATA_SOURCE = CuratedDataSource,
    FILE_FORMAT = ParquetFormat
)
AS
WITH daily_returns AS (
    SELECT * FROM CORE.int_stock_daily
    WHERE daily_return IS NOT NULL
),

drawdown_calc AS (
    SELECT
        symbol,
        date,
        [close],
        MAX([close]) OVER (
            PARTITION BY symbol
            ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS rolling_max
    FROM daily_returns
),
max_drawdown AS (
    SELECT
        symbol,
        MAX((rolling_max - [close]) / NULLIF(rolling_max, 0)) AS max_drawdown
    FROM drawdown_calc
    GROUP BY symbol
),

var95_calc AS (
    SELECT DISTINCT
        symbol,
        PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY daily_return) OVER (PARTITION BY symbol) AS var_95
    FROM daily_returns
),

risk_calc AS (
    SELECT
        symbol,
        COUNT(*) AS trading_days,
        AVG(daily_return) AS avg_daily_return,
        STDEV(daily_return) AS daily_volatility,
        STDEV(daily_return) * SQRT(252) AS annual_volatility,
        MIN(daily_return) AS max_daily_loss,
        MAX(daily_return) AS max_daily_gain,
        AVG(daily_return) / NULLIF(STDEV(daily_return), 0) * SQRT(252) AS sharpe_ratio
    FROM daily_returns
    GROUP BY symbol
)

SELECT
    r.symbol,
    r.trading_days,
    ROUND(r.avg_daily_return * 100, 4) AS avg_daily_return_pct,
    ROUND(r.daily_volatility * 100, 4) AS daily_volatility_pct,
    ROUND(r.annual_volatility * 100, 4) AS annual_volatility_pct,
    ROUND(r.max_daily_loss * 100, 4) AS max_daily_loss_pct,
    ROUND(r.max_daily_gain * 100, 4) AS max_daily_gain_pct,
    ROUND(v.var_95 * 100, 4) AS var_95_pct,
    ROUND(r.sharpe_ratio, 4) AS sharpe_ratio,
    ROUND(d.max_drawdown * 100, 4) AS max_drawdown_pct
FROM risk_calc r
LEFT JOIN max_drawdown d ON r.symbol = d.symbol
LEFT JOIN var95_calc v ON r.symbol = v.symbol
ORDER BY annual_volatility_pct DESC;

SELECT * FROM MARTS.mart_risk_metrics
ORDER BY annual_volatility_pct DESC;