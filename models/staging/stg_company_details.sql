{{ config(materialized='view') }}

SELECT
    Symbol AS ticker,
    Sector,
    Industry,
    Marketcap,
    Currentprice,
    Ebitda,
    Revenuegrowth,
    Country
FROM {{ source('raw', 'companies_details_raw') }}