{{ config(materialized='view') }}

with base as (
    select * from {{ ref('fct_stock_performance') }}
),

latest_date as (
    select max(trade_date) as recent_date from base
),

ranked as (
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
        row_number() over ( partition by case
                when b.daily_percent_change > 0 then 'Gainer'
                when b.daily_percent_change < 0 then 'Loser'
            end order by abs(b.daily_percent_change) desc
        ) as rank
    from base b
    join latest_date l on b.trade_date = l.recent_date
)

select * from ranked where rank <= 10