{{
    config(
        materialized='table'
    )
}}

{% set window_partition = "PARTITION BY ticker ORDER BY trade_date" %}
{% set days_30 = "ROWS BETWEEN 29 PRECEDING AND CURRENT ROW" %}
{% set days_90 = "ROWS BETWEEN 89 PRECEDING AND CURRENT ROW" %}
{% set days_200 = "ROWS BETWEEN 199 PRECEDING AND CURRENT ROW" %}
{% set weeks_52 = "ROWS BETWEEN 364 PRECEDING AND CURRENT ROW" %} 

WITH source_stg AS (
    SELECT
        trade_date,
        ticker, 
        high_price,
        low_price,
        close_price,
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
        {{ window_partition }} {{ days_30 }}
    ) AS moving_avg_30d,

    AVG(close_price) OVER (
        {{ window_partition }} {{ days_90 }}
    ) AS moving_avg_90d,

    AVG(close_price) OVER (
        {{ window_partition }} {{ days_200 }}
    ) AS moving_avg_200d,

    
    STDDEV(daily_percent_change) OVER (
        {{ window_partition }} {{ days_30 }}
    ) AS volatility_30d,

   
    MAX(high_price) OVER (
        {{ window_partition }} {{ weeks_52 }}
    ) AS high_52w,

   
    MIN(low_price) OVER (
        {{ window_partition }} {{ weeks_52 }}
    ) AS low_52w

FROM source_stg