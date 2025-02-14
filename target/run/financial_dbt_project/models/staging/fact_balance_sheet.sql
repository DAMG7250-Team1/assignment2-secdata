
  
    

        create or replace transient table ASSIGNMENT2_TEAM1.FACT_TABLE_STAGING_FACT_TABLE_STAGING.fact_balance_sheet
         as
        (

SELECT
    n.adsh,
    n.tag,
    n.value,
    p.stmt,
    p.plabel
FROM ASSIGNMENT2_TEAM1.FACT_TABLE_STAGING_FACT_TABLE_STAGING.stg_num n
JOIN ASSIGNMENT2_TEAM1.FACT_TABLE_STAGING_FACT_TABLE_STAGING.stg_pre p
ON n.adsh = p.adsh AND n.tag = p.tag
WHERE p.stmt = 'BS'
        );
      
  