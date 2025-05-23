{% set file_name = var('file_name', '2023_4') %}

{{ config(
    alias='INCOME_STATEMENT_' ~ file_name,
    schema='denormalize_fact_staging',  -- Set schema here
    materialized='table'
) }}

-- Debug log
{% do log('Table name: ' ~ table_name, info=True) %}


WITH FilteredData AS (
    SELECT 
        num.adsh, 
        sub.cik, 
        sub.name AS company_name,
        sub.filed AS filing_date, 
        sub.fy AS fiscal_year, 
        sub.fp AS fiscal_period, 
        num.tag, 
        num.uom AS unit_of_measure, 
        num.ddate AS report_date, 
        num.qtrs, 
        pre.stmt AS statement_type, 
        pre.plabel,
        DENSE_RANK() OVER (PARTITION BY 
            num.adsh, sub.cik, sub.name, sub.filed, sub.fy, sub.fp, 
            num.tag, num.uom, num.ddate, num.qtrs, pre.stmt, pre.plabel 
            ORDER BY num.ddate DESC
        ) AS rn,
        num.value
    FROM {{ ref('raw_num') }} as num  -- Dynamically include the stage name
    JOIN {{ ref('raw_sub') }} as sub  -- Dynamically include the stage name
        ON num.adsh = sub.adsh
    JOIN {{ ref('raw_pre') }} as pre  -- Dynamically include the stage name
        ON num.adsh = pre.adsh 
        AND num.tag = pre.tag
    WHERE pre.stmt = 'IS' 
)

SELECT 
    adsh, 
    cik, 
    company_name,
    filing_date, 
    fiscal_year, 
    fiscal_period, 
    tag, 
    unit_of_measure, 
    report_date, 
    qtrs, 
    statement_type, 
    plabel, 
    SUM(value) AS total_value
FROM FilteredData
GROUP BY adsh, cik, company_name, filing_date, fiscal_year, fiscal_period, 
         tag, unit_of_measure, report_date, qtrs, statement_type, plabel, rn
