with DATAAREA as(

    select * from {{ref('src_dataarea')}}

),

LEDGER as 
(
    select * from {{ref('src_ledger')}}
),

FINAL as 
(
    SELECT '{{env_var("DBT_SOURCE_SYSTEM")}}'             AS SOURCE_SYSTEM,
           UPPER (TRIM (DA.ID))    AS SOURCE_SYSTEM_ADDRESS_NUMBER,
           'COMPANY'               AS GENERIC_ADDRESS_TYPE,
           DA.NAME                 AS SOURCE_COMPANY_NAME,
           LDGR.ACCOUNTINGCURRENCY AS DEFAULT_CURRENCY_CODE,
           LDGR.REPORTINGCURRENCY  AS PARENT_CURRENCY_CODE,
           'COMPANY'               AS TYPE,
           UPPER (TRIM (DA.ID))    AS DIVISION,
           'PHI'                   AS REGION,
           NULL                    AS SEGMENT
      FROM DATAAREA DA,LEDGER LDGR
     WHERE UPPER (TRIM (DA.ID)) <> 'DAT' AND DA.ID = LDGR.NAME(+)

)

select {{ dbt_utils.surrogate_key(['SOURCE_SYSTEM','SOURCE_SYSTEM_ADDRESS_NUMBER','GENERIC_ADDRESS_TYPE']) }} AS COMPANY_ADDRESS_GUID ,* from Final