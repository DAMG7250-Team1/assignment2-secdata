

SELECT
    n.adsh,
    n.tag,
    n.value,
    p.stmt,
    p.plabel
FROM ASSIGNMENT2_TEAM1.FACT_TABLE_STAGING_DENORMALIZE_FACT_STAGING.stg_num n
JOIN ASSIGNMENT2_TEAM1.FACT_TABLE_STAGING_DENORMALIZE_FACT_STAGING.stg_pre p
ON n.adsh = p.adsh AND n.tag = p.tag
WHERE p.stmt = 'BS'