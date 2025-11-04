{{ config(materialized='view') }}

select
    ticker,
    trade_date,
    close_price as current_price,
    moving_avg_30d as ma_30d,
    moving_avg_200d as ma_200d,
    case
        when moving_avg_30d > moving_avg_200d then 'Golden Cross'
        when moving_avg_30d < moving_avg_200d then 'Death Cross'
        else 'Neutral'
    end as signal
from {{ ref('fct_stock_performance') }}