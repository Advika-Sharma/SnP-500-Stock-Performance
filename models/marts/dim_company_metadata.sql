{{ config(materialized='view') }}

WITH stg_details AS (
    -- Select all columns from the staging model after filtering
    SELECT
        ticker,
        sector,
        industry,
        marketcap,
        currentprice,
        ebitda,
        revenuegrowth,
        country
    FROM {{ ref('stg_company_details') }}
    WHERE ticker IS NOT NULL
)

SELECT
    -- Pass-through columns using target naming convention (e.g., snake_case)
    ticker,
    sector,
    industry,
    marketcap AS market_cap,  -- Renaming for consistency
    currentprice AS current_price,  -- Renaming for consistency
    ebitda,
    revenuegrowth,
    country,

    -- Calculated final column
    ROUND(ebitda / NULLIF(marketcap, 0), 2) AS pe_ratio
FROM stg_details
