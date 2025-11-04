{{ config(materialized='view') }}

with yearly as (
    select
        ticker,
        date_part('year', trade_date) as year,
        first_value(close_price) over (
            partition by ticker, date_part('year', trade_date)
            order by trade_date
        ) as start_of_year_price,
        last_value(close_price) over (
            partition by ticker, date_part('year', trade_date)
            order by trade_date
            rows between unbounded preceding and unbounded following
        ) as end_of_year_price,
        (
            (
                last_value(close_price) over (
                    partition by ticker, date_part('year', trade_date)
                    order by trade_date
                    rows between unbounded preceding and unbounded following
                )
                - first_value(close_price) over (
                    partition by ticker, date_part('year', trade_date)
                    order by trade_date
                )
            )
            / first_value(close_price) over (
                partition by ticker, date_part('year', trade_date)
                order by trade_date
            )
        ) * 100 as annual_return_percent,
        (
            (
                max(high_52w) over (partition by ticker, date_part('year', trade_date))
                - min(low_52w) over (partition by ticker, date_part('year', trade_date))
            ) / nullif(max(high_52w) over (partition by ticker, date_part('year', trade_date)), 0)
        ) * 100 as max_drawdown_percent,
        row_number() over (
            partition by ticker, date_part('year', trade_date)
            order by trade_date desc
        ) as rn
    from {{ ref('fct_stock_performance') }}
)

select
    ticker, year, start_of_year_price, end_of_year_price, annual_return_percent, max_drawdown_percent
from yearly
where rn = 1
