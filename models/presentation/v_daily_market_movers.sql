{{ config(materialized='view') }}


with base as (
    select * 
    from {{ ref('fct_stock_performance') }}
),

latest_date as (
    select max(trade_date) as recent_date 
    from base
),

--rank gainers and losers based on daily_percent_change
ranked_changes as (
    select
        b.ticker,
        b.close_price,
        b.daily_percent_change,
        b.volume,
        case
            when b.daily_percent_change > 0 then 'Gainer'
            when b.daily_percent_change < 0 then 'Loser'
            else 'No Change'
        end as category,
        row_number() over (
            partition by case
                when b.daily_percent_change > 0 then 'Gainer'
                when b.daily_percent_change < 0 then 'Loser'
                else 'No Change'
            end
            order by abs(b.daily_percent_change) desc
        ) as rank
    from base b
    join latest_date l 
        on b.trade_date = l.recent_date
    where b.daily_percent_change is not null
),

--rank most traded stocks by volume
ranked_volume as (
    select
        b.ticker,
        b.close_price,
        b.daily_percent_change,
        b.volume,
        'Most Traded' as category,
        row_number() over (order by b.volume desc) as rank
    from base b
    join latest_date l 
        on b.trade_date = l.recent_date
)

--combining all categories and keeping only top 10 from each
select
    ticker,
    close_price,
    daily_percent_change,
    volume,
    category
from (
    select * from ranked_changes where rank <= 10
    union all
    select * from ranked_volume where rank <= 10
) final
order by category, rank