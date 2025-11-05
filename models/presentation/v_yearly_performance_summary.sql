{{ config(materialized='view') }}

with yearly_bounds as (
    select
        ticker,
        date_part('year', trade_date) as year,
        min(trade_date) as start_date,
        max(trade_date) as end_date
    from {{ ref('fct_stock_performance') }}
    group by 1, 2
),

prices as (
    select
        y.ticker,
        y.year,
        f.close_price as start_of_year_price,
        e.close_price as end_of_year_price
    from yearly_bounds y
    left join {{ ref('fct_stock_performance') }} f
        on y.ticker = f.ticker and y.start_date = f.trade_date
    left join {{ ref('fct_stock_performance') }} e
        on y.ticker = e.ticker and y.end_date = e.trade_date
),

metrics as (
    select
        p.ticker,
        p.year,
        p.start_of_year_price,
        p.end_of_year_price,
        ((p.end_of_year_price - p.start_of_year_price) / nullif(p.start_of_year_price, 0)) * 100 as annual_return_percent,
        ((max(f.high_52w) - min(f.low_52w)) / nullif(max(f.high_52w), 0)) * 100 as max_drawdown_percent
    from prices p
    join {{ ref('fct_stock_performance') }} f
        on p.ticker = f.ticker
        and date_part('year', f.trade_date) = p.year
    group by 1, 2, 3, 4
)

select * from metrics