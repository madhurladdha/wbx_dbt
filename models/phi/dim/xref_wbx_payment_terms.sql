{{
    config(
    materialized = env_var('DBT_MAT_INCREMENTAL'),
    transient = false,
    unique_key = 'UNIQUE_KEY',
    on_schema_change='sync_all_columns',
    tags = "rdm_core"
    )
}}

with stg_payment_terms as (
    SELECT * FROM {{ ref('stg_d_wbx_payment_terms') }}
),
hist_payment_terms as (
    select * from {{ ref('conv_xref_wbx_payment_terms') }}
),

casting as (
    SELECT 
        CAST (SOURCE_SYSTEM AS VARCHAR2 (30 BYTE))              AS SOURCE_SYSTEM,
        CAST (SOURCE_PAYMENT_TERMS_CODE AS VARCHAR2 (30 BYTE))  AS SOURCE_PAYMENT_TERMS_CODE,
        CAST (PAYMENT_TERMS_CODE AS VARCHAR2 (30 BYTE))         AS PAYMENT_TERMS_CODE,
        CAST (PAYMENT_TERMS_DESCRIPTION AS VARCHAR2 (255 BYTE)) AS PAYMENT_TERMS_DESCRIPTION,
        CAST (DAYS_TO_PAY AS INTEGER)                           AS DAYS_TO_PAY,
        CAST (DAYS_TO_DISCOUNT AS INTEGER)                      AS DAYS_TO_DISCOUNT,
        CAST (DISCOUNT_PERCENT AS DECIMAL(15,4))                AS DISCOUNT_PERCENT
    FROM stg_payment_terms
),

payment_terms as (
    select
        {{ dbt_utils.surrogate_key(['SRC.SOURCE_SYSTEM','SRC.SOURCE_PAYMENT_TERMS_CODE']) }} AS PAYMENT_TERMS_GUID,
        null as PAYMENT_TERMS_GUID_OLD,
        SRC.SOURCE_SYSTEM,
        SRC.SOURCE_PAYMENT_TERMS_CODE,
        SRC.DAYS_TO_PAY,
        SRC.DAYS_TO_DISCOUNT,
        SRC.DISCOUNT_PERCENT,
        NVL(TO_CHAR(PAYMENT_TERMS_CODE_LKP.NORMALIZED_VALUE),TO_CHAR(SRC.PAYMENT_TERMS_CODE)) AS PAYMENT_TERMS_CODE,
        NVL(TO_CHAR(PAYMENT_TERMS_DESC_LKP.NORMALIZED_VALUE),TO_CHAR(SRC.PAYMENT_TERMS_DESCRIPTION)) AS PAYMENT_TERMS_DESCRIPTION,
        SYSTIMESTAMP() as LOAD_DATE,
        SYSTIMESTAMP() as UPDATE_DATE
    from casting SRC
        LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ADDRESS_BOOK','PAYMENT_TERMS_CODE','SRC.PAYMENT_TERMS_CODE','PAYMENT_TERMS_CODE_LKP') }}
        LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ADDRESS_BOOK','PAYMENT_TERMS_DESC','SRC.SOURCE_PAYMENT_TERMS_CODE','PAYMENT_TERMS_DESC_LKP') }}
),
passthrough as (
    SELECT         
        {{ dbt_utils.surrogate_key(['payment_terms_guid']) }} as UNIQUE_KEY,
        *
    from payment_terms
),

new_dim  as 
(
select
cast(substring(A.source_system,1,30) as text(30) ) as source_system  ,
cast(A.payment_terms_guid as text(255) ) as payment_terms_guid  ,
cast(B.payment_terms_guid_old as text(255) ) as payment_terms_guid_old  ,
cast(substring(A.source_payment_terms_code,1,30) as text(30) ) as source_payment_terms_code  ,
cast(substring(A.payment_terms_code,1,30) as text(30) ) as payment_terms_code  ,
cast(substring(A.payment_terms_description,1,60) as text(60) ) as payment_terms_description  ,
cast(A.days_to_pay as number(38,0) ) as days_to_pay  ,
cast(A.days_to_discount as number(38,0) ) as days_to_discount  ,
cast(A.discount_percent as number(38,10) ) as discount_percent  ,
cast(A.load_date as timestamp_ntz(9) ) as load_date  ,
cast(A.update_date as timestamp_ntz(9) ) as update_date  ,
cast(A.unique_key as text(255) ) as unique_key
from passthrough A
LEFT JOIN hist_payment_terms B ON A.PAYMENT_TERMS_GUID = B.PAYMENT_TERMS_GUID
),

old_dim as(
select
cast(substring(A.source_system,1,30) as text(30) ) as source_system  ,
cast(A.payment_terms_guid as text(255) ) as payment_terms_guid  ,
cast(A.payment_terms_guid_old as text(255) ) as payment_terms_guid_old  ,
cast(substring(A.source_payment_terms_code_NEW,1,30) as text(30) ) as source_payment_terms_code  ,
cast(substring(A.payment_terms_code,1,30) as text(30) ) as payment_terms_code  ,
cast(substring(A.payment_terms_description,1,60) as text(60) ) as payment_terms_description  ,
cast(A.days_to_pay as number(38,0) ) as days_to_pay  ,
cast(A.days_to_discount as number(38,0) ) as days_to_discount  ,
cast(A.discount_percent as number(38,10) ) as discount_percent  ,
cast(A.load_date as timestamp_ntz(9) ) as load_date  ,
cast(A.update_date as timestamp_ntz(9) ) as update_date  ,
cast(A.unique_key as text(255) ) as unique_key
from hist_payment_terms A
left JOIN  passthrough B ON A.PAYMENT_TERMS_GUID = B.PAYMENT_TERMS_GUID
where B.PAYMENT_TERMS_GUID is null
),

final as (
    select * from old_dim
    union
    select * from new_dim
)

SELECT * FROM final	