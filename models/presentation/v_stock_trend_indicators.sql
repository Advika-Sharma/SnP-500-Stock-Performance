{{ config(materialized='view') }}

select
    ticker,
    trade_date,
    close_price as current_price,
    moving_avg_30d as ma_30d,
    moving_avg_200d as ma_200d,
    CASE
        WHEN moving_avg_30d > moving_avg_200d AND 
            LAG(moving_avg_30d, 1) OVER (PARTITION BY ticker ORDER BY trade_date) <= 
            LAG(moving_avg_200d, 1) OVER (PARTITION BY ticker ORDER BY trade_date) THEN 'Golden Cross'
        WHEN moving_avg_30d < moving_avg_200d AND 
            LAG(moving_avg_30d, 1) OVER (PARTITION BY ticker ORDER BY trade_date) >= 
            LAG(moving_avg_200d, 1) OVER (PARTITION BY ticker ORDER BY trade_date) THEN 'Death Cross'
        ELSE 'No Signal'
    END AS signal
from {{ ref('fct_stock_performance') }}