{{
    config(
    materialized = env_var('DBT_MAT_INCREMENTAL'),
    transient = false,
    unique_key = 'UNIQUE_KEY',
    on_schema_change='sync_all_columns',
    tags = "rdm_core"
    )
}}
with STG_COMPANY as (
    SELECT * FROM {{ ref('stg_d_wbx_company') }}
),
HIST_COMPANY as (
    select * from {{ ref('conv_dim_wbx_company') }}
),
INT_COMPANY AS (
    SELECT
        SRC.GENERIC_ADDRESS_TYPE,
        SRC.COMPANY_ADDRESS_GUID,
        {{ dbt_utils.surrogate_key(['SRC.COMPANY_ADDRESS_GUID']) }} as UNIQUE_KEY,
        null as COMPANY_ADDRESS_GUID_OLD,
        SRC.SOURCE_SYSTEM,
        SRC.TYPE,
        NVL(TO_CHAR(DIVISION_LKP.NORMALIZED_VALUE),TO_CHAR(SRC.DIVISION))      AS DIVISION,
        SRC.REGION,
        SRC.SOURCE_SYSTEM_ADDRESS_NUMBER AS  COMPANY_CODE,
        SRC.SOURCE_COMPANY_NAME AS COMPANY_NAME,
        NVL(TO_CHAR(OPERATING_COMPANY_LKP.NORMALIZED_VALUE),TO_CHAR(SRC.SOURCE_SYSTEM_ADDRESS_NUMBER))  as OPERATING_COMPANY,
        NVL(TO_CHAR(SEGMENT_LKP.NORMALIZED_VALUE),TO_CHAR(SRC.SOURCE_SYSTEM_ADDRESS_NUMBER))  as SEGMENT,
        DEFAULT_CURRENCY_CODE,
        SYSTIMESTAMP() AS DATE_INSERTED,
        SYSTIMESTAMP() AS DATE_UPDATED,
        CASE WHEN
            SRC.SOURCE_SYSTEM_ADDRESS_NUMBER ='Y' 
            THEN 'Y' 
            ELSE 'N' 
            END AS ETL_EXCLUDE_FLAG,
        PARENT_CURRENCY_CODE,
        SRC.TYPE AS ADDRESS_TYPE
    FROM STG_COMPANY SRC
    LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ADDRESS_BOOK','DIVISION','UPPER(SRC.DIVISION)','DIVISION_LKP') }} 
    LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ADDRESS_BOOK','CC_OPERATING_COMPANY','UPPER(SRC.SOURCE_SYSTEM_ADDRESS_NUMBER)','OPERATING_COMPANY_LKP') }} 
    LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ADDRESS_BOOK','CC_SEGMENT','UPPER(SRC.SOURCE_SYSTEM_ADDRESS_NUMBER)','SEGMENT_LKP') }} 
        ),

new_dim as (
SELECT 
        CAST(substr(A.UNIQUE_KEY,1,255) as VARCHAR2(255))                                AS UNIQUE_KEY,
        CAST(A.GENERIC_ADDRESS_TYPE AS VARCHAR(255))                                     AS GENERIC_ADDRESS_TYPE,
        CAST(substr(A.COMPANY_ADDRESS_GUID,1,255) AS VARCHAR2(255))                      AS COMPANY_ADDRESS_GUID,
        CAST(nvl(A.COMPANY_ADDRESS_GUID_OLD,B.COMPANY_ADDRESS_GUID_OLD) AS NUMBER(38,0)) AS COMPANY_ADDRESS_GUID_OLD,
        CAST(substr(A.SOURCE_SYSTEM,1,255) as VARCHAR2(255))                             AS SOURCE_SYSTEM,
        CAST(substr(A.TYPE,1,255) as VARCHAR2(255))                                      AS TYPE,
        CAST(substr(A.ADDRESS_TYPE,1,255) as VARCHAR2(255))                              AS ADDRESS_TYPE,
        CAST(substr(A.DIVISION,1,255) as VARCHAR2(255))                                  AS DIVISION,
        CAST(substr(A.REGION,1,255) as VARCHAR2(255))                                    AS REGION,
        CAST(substr(A.COMPANY_CODE,1,255) as VARCHAR2(255))                              AS COMPANY_CODE,
        CAST(substr(A.COMPANY_NAME,1,255) as VARCHAR2(255))                              AS COMPANY_NAME,
        CAST(substr(A.OPERATING_COMPANY,1,255) as VARCHAR2(255))                         AS OPERATING_COMPANY,
        CAST(substr(A.SEGMENT,1,255) as VARCHAR2(255))                                   AS SEGMENT,
        CAST(substr(A.DEFAULT_CURRENCY_CODE,1,255) as VARCHAR2(255))                     AS DEFAULT_CURRENCY_CODE,
        cast(substr(A.DATE_INSERTED ,1,255)  as VARCHAR2(255))                           AS DATE_INSERTED,
        cast(substr(A.DATE_UPDATED ,1,255)  as VARCHAR2(255))                            AS DATE_UPDATED,
        CAST(substr(A.PARENT_CURRENCY_CODE,1,30)  as VARCHAR(30))                        AS PARENT_CURRENCY_CODE
    FROM INT_COMPANY A
    LEFT JOIN HIST_COMPANY B
        ON A.COMPANY_ADDRESS_GUID=B.COMPANY_ADDRESS_GUID
),

old_dim as (
    SELECT 
        CAST(substr(A.UNIQUE_KEY,1,255) as VARCHAR2(255))                                AS UNIQUE_KEY,
        CAST(A.GENERIC_ADDRESS_TYPE AS VARCHAR(255))                                     AS GENERIC_ADDRESS_TYPE,
        CAST(substr(A.COMPANY_ADDRESS_GUID,1,255) AS VARCHAR2(255))                      AS COMPANY_ADDRESS_GUID,
        CAST(B.COMPANY_ADDRESS_GUID_OLD AS NUMBER(38,0))                                 AS COMPANY_ADDRESS_GUID_OLD,
        CAST(substr(A.SOURCE_SYSTEM,1,255) as VARCHAR2(255))                             AS SOURCE_SYSTEM,
        CAST(substr(A.TYPE,1,255) as VARCHAR2(255))                                      AS TYPE,
        CAST(substr(A.TYPE,1,255) as VARCHAR2(255))                                      AS ADDRESS_TYPE,
        CAST(substr(A.DIVISION,1,255) as VARCHAR2(255))                                  AS DIVISION,
        CAST(substr(A.REGION,1,255) as VARCHAR2(255))                                    AS REGION,
        CAST(substr(A.COMPANY_CODE,1,255) as VARCHAR2(255))                              AS COMPANY_CODE,
        CAST(substr(A.COMPANY_NAME,1,255) as VARCHAR2(255))                              AS COMPANY_NAME,
        CAST(substr(A.OPERATING_COMPANY,1,255) as VARCHAR2(255))                         AS OPERATING_COMPANY,
        CAST(substr(A.SEGMENT,1,255) as VARCHAR2(255))                                   AS SEGMENT,
        CAST(substr(A.DEFAULT_CURRENCY_CODE,1,255) as VARCHAR2(255))                     AS DEFAULT_CURRENCY_CODE,
        cast(substr(A.DATE_INSERTED ,1,255)  as VARCHAR2(255))                           AS DATE_INSERTED,
        cast(substr(A.DATE_UPDATED ,1,255)  as VARCHAR2(255))                            AS DATE_UPDATED,
        CAST(substr(A.PARENT_CURRENCY_CODE,1,30)  as VARCHAR(30))                        AS PARENT_CURRENCY_CODE
    FROM HIST_COMPANY A
    LEFT JOIN INT_COMPANY B
    ON A.COMPANY_ADDRESS_GUID=B.COMPANY_ADDRESS_GUID
    WHERE B.COMPANY_ADDRESS_GUID is NULL),

Final_Dim
as
(SELECT * FROM new_dim
UNION
SELECT * FROM old_dim)

Select * from Final_Dim