{{ config(materialized='view') }}

SELECT
    ticker,
    sector,
    industry,
    marketcap AS market_cap,
    currentprice AS current_price,
    ROUND(ebitda / NULLIF(marketcap, 0), 2) AS pe_ratio
FROM {{ ref('stg_company_details') }}
WHERE ticker IS NOT NULL