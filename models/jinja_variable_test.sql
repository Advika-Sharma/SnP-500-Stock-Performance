{% set threshold = 100 %}

select *
from {{ ref('stg_stock_prices') }}
where high_price > {{ threshold }}