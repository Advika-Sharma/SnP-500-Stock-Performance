{{ config( materialized = 'table' ) }}

with base as (
    select
        trade_date,
        ticker,
        close_price,
        open_price,
        high_price,
        low_price,
        volume,
        daily_change,
        daily_percent_change
    from {{ ref('stg_stock_prices') }}
),
calc as (
    select
        trade_date,
        ticker,
        close_price,
        open_price,
        high_price,
        low_price,
        volume,
        daily_change,
        daily_percent_change,

        -- Moving averages
        avg(close_price) over (
            partition by ticker order by trade_date rows between 29 preceding and current row
        ) as moving_avg_30d,

        avg(close_price) over (
            partition by ticker order by trade_date rows between 89 preceding and current row
        ) as moving_avg_90d,

        avg(close_price) over (
            partition by ticker order by trade_date rows between 199 preceding and current row
        ) as moving_avg_200d,

        -- 30-day rolling volatility (stddev of daily_percent_change)
        stddev(daily_percent_change) over (
            partition by ticker order by trade_date rows between 29 preceding and current row
        ) as volatility_30d,

        -- 52-week (365 day) high/low using window of 364 preceding rows
        max(high_price) over (
            partition by ticker order by trade_date rows between 364 preceding and current row
        ) as high_52w,

        min(low_price) over (
            partition by ticker order by trade_date rows between 364 preceding and current row
        ) as low_52w
    from base
)

select * from calc  order by ticker, trade_date