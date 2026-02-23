select
high_price,
low_price,
    {{ calculate_market_cap('high_price', 'low_price') }} as market_cap
from {{ ref('stg_stock_prices') }}