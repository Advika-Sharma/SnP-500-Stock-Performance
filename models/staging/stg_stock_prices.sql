WITH source_data AS (
    SELECT
        TO_DATE(DATE) AS trade_date,
        CAST(OPEN AS NUMBER(12,4)) AS open_price,
        CAST(HIGH AS NUMBER(12,4)) AS high_price,
        CAST(LOW AS NUMBER(12,4)) AS low_price,
        CAST(CLOSE AS NUMBER(12,4)) AS close_price,
        CAST(VOLUME AS NUMBER(20,0)) AS volume,
        NAME AS ticker
    FROM {{ source('raw', 'raw_stock_prices') }}
),

final AS (
    SELECT
        *,
        close_price - LAG(close_price) OVER (PARTITION BY ticker ORDER BY trade_date) AS daily_change,
        ROUND(
            (close_price - LAG(close_price) OVER (PARTITION BY ticker ORDER BY trade_date)) 
            / NULLIF(LAG(close_price) OVER (PARTITION BY ticker ORDER BY trade_date), 0) * 100,
            4
        ) AS daily_percent_change
    FROM source_data
)




SELECT * FROM final

