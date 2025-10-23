{{ config(
    materialized='table'
) }}

WITH base AS (
    SELECT
        trade_date,
        ticker,
        close_price,
        high_price,
        low_price,
        volume,
        daily_percent_change
    FROM {{ ref('stg_stock_prices') }}
)

SELECT
    trade_date,
    ticker,
    close_price,
    volume,
    daily_percent_change,

    AVG(close_price) OVER (
        PARTITION BY ticker
        ORDER BY trade_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS moving_avg_30d,

    AVG(close_price) OVER (
        PARTITION BY ticker
        ORDER BY trade_date
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) AS moving_avg_90d,

    AVG(close_price) OVER (
        PARTITION BY ticker
        ORDER BY trade_date
        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
    ) AS moving_avg_200d,

    STDDEV(daily_percent_change) OVER (
        PARTITION BY ticker
        ORDER BY trade_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS volatility_30d,

    
    MAX(high_price) OVER (
        PARTITION BY ticker
        ORDER BY trade_date
        ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS high_52w,

    MIN(low_price) OVER (
        PARTITION BY ticker
        ORDER BY trade_date
        ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS low_52w

FROM base
ORDER BY ticker, trade_date
