{{ config(materialized='view') }}

WITH stg_details AS (
    SELECT
        ticker,
        sector,
        industry,
        market_cap,    -- Use the alias from stg_company_details
        current_price, -- Use the alias from stg_company_details
        ebitda,
        revenue_growth, -- Use the alias from stg_company_details
        country
    FROM {{ ref('stg_company_details') }}
    WHERE ticker IS NOT NULL
)

SELECT
    ticker,
    sector,
    industry,
    market_cap,
    current_price,
    ebitda,
    revenue_growth,
    country,

    -- Use the correct alias in your calculation
    ROUND(ebitda / NULLIF(market_cap, 0), 2) AS pe_ratio
FROM stg_details

