{{ config(materialized='view') }}

select
    f.ticker,
    c.sector,
    c.industry,
    c.market_cap,
    f.close_price as current_price,
    c.pe_ratio
from {{ ref('fct_stock_performance') }} f
join {{ ref('dim_company_metadata') }} c
    on f.ticker = c.ticker 