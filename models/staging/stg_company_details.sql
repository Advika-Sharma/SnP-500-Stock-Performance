{{ config(materialized='view') }}

SELECT
    "Symbol" AS ticker,
    "Sector" AS sector,
    "Industry" AS industry,
    "Marketcap" AS market_cap,
    "Currentprice" AS current_price,
    "Ebitda" AS ebitda,
    "Revenuegrowth" AS revenue_growth,
    "Country" AS country
FROM {{ source('raw', 'companies_details_raw') }}