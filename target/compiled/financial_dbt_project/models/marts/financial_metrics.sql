

WITH balance_sheet AS (
    SELECT *
    FROM ASSIGNMENT2_TEAM1.FACT_TABLE_STAGING_DENORMALIZE_FACT_STAGING.fact_balance_sheet
),

metrics AS (
    SELECT 
        adsh,
        tag,
        value,
        stmt,
        plabel,
        SUM(value) OVER (PARTITION BY adsh) as total_value
    FROM balance_sheet
)

SELECT 
    adsh,
    tag,
    value,
    stmt,
    plabel,
    total_value,
    (value / NULLIF(total_value, 0)) * 100 as percentage_of_total
FROM metrics