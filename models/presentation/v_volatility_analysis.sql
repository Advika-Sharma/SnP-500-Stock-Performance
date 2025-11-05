{{ config(materialized='view') }}

select
    ticker,
    trade_date,
    volatility_30d as rolling_volatility_30d,
    (high_52w - low_52w) as range_52_week
from {{ ref('fct_stock_performance') }}
