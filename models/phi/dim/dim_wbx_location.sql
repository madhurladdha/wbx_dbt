{{
    config(
    materialized = env_var('DBT_MAT_INCREMENTAL'),
    transient = false,
    unique_key = 'UNIQUE_KEY',
    on_schema_change='sync_all_columns',
    tags="rdm_core"
    )
}}

with stg_location as (
    select * from {{ ref('stg_d_wbx_location') }}
),

hist_loc as 
(
    select * from {{ ref('conv_dim_wbx_location') }}
),

plant_master as(
    select * from {{ref('dim_wbx_plant_dc')}}
),



int_location as (
    SELECT 
        {{ dbt_utils.surrogate_key(['A.SOURCE_SYSTEM','A.SOURCE_LOCATION_CODE','A.SOURCE_BUSINESS_UNIT_CODE']) }} as location_guid,
        NULL as location_guid_old,
        {{ dbt_utils.surrogate_key(['A.SOURCE_SYSTEM','A.SOURCE_BUSINESS_UNIT_CODE',"'PLANT_DC'"]) }} as business_unit_address_guid,
        NULL as business_unit_address_guid_old,
        A.SOURCE_LOCATION_CODE as SOURCE_LOCATION_CODE,
        A.SOURCE_BUSINESS_UNIT_CODE as SOURCE_BUSINESS_UNIT_CODE,
        A.SOURCE_SYSTEM as SOURCE_SYSTEM,
        A.SOURCE_AISLE_CODE as SOURCE_AISLE_CODE,
        A.SOURCE_BIN_CODE as SOURCE_BIN_CODE,
        A.STAGING_LOCATION_FLAG as STAGING_LOCATION_FLAG,
        systimestamp() as load_date,
        systimestamp() as update_date
    FROM stg_location A
    join plant_master B on  A.SOURCE_SYSTEM = B.SOURCE_SYSTEM
  and A.SOURCE_BUSINESS_UNIT_CODE = coalesce(trim(B.SOURCE_BUSINESS_UNIT_CODE),'-')
),

UNIQUE_PASSTHROUGH as 
(
    SELECT  {{ dbt_utils.surrogate_key(['location_guid']) }} as UNIQUE_KEY,* from int_location
),

NEW_DIM as (

SELECT
        CAST( substr(   A.UNIQUE_KEY                        ,1,255)   as VARCHAR2(255))       as UNIQUE_KEY,
        CAST( substr(   A.source_location_code              ,1,60)    as VARCHAR(60))         as source_location_code,
        CAST( substr(   A.source_business_unit_code         ,1,60)    as VARCHAR(60))         as source_business_unit_code,
        CAST( substr(   A.source_system                     ,1,30)    as VARCHAR(30))         as source_system,
        CAST( substr(   A.location_guid                     ,1,255)   as VARCHAR2(255))       as location_guid,
        CAST(           A.location_guid_old                           as VARCHAR2(255))       as location_guid_old,
        CAST( substr(   A.business_unit_address_guid        ,1,255)   as VARCHAR2(255))       as business_unit_address_guid,
        CAST(           A.business_unit_address_guid_old              as VARCHAR2(255))       as business_unit_address_guid_old,
        CAST( substr(   A.source_aisle_code                 ,1,60)    as VARCHAR(60))         as source_aisle_code,
        CAST( substr(   A.source_bin_code                   ,1,60)    as VARCHAR(60))         as source_bin_code,
        CAST( substr(   A.staging_location_flag             ,1,1)     as VARCHAR(1))          as staging_location_flag,
        CAST(           A.load_date                                   as TIMESTAMP_NTZ(9))    as load_date,
        CAST(           A.update_date                                 as TIMESTAMP_NTZ(9))    as update_date
    from UNIQUE_PASSTHROUGH A
    
),
OLD_DIM as (
    SELECT
        CAST( substr(   A.UNIQUE_KEY                        ,1,255)   as VARCHAR2(255))       as UNIQUE_KEY,
        CAST( substr(   A.source_location_code              ,1,60)    as VARCHAR(60))         as source_location_code,
        CAST( substr(   A.source_business_unit_code_new         ,1,60)    as VARCHAR(60))         as source_business_unit_code,
        CAST( substr(   A.source_system                     ,1,30)    as VARCHAR(30))         as source_system,
        CAST( substr(   A.location_guid                     ,1,255)   as VARCHAR2(255))       as location_guid,
        CAST(           a.location_guid_old                           as VARCHAR2(255))        as location_guid_old,
        CAST( substr(   A.business_unit_address_guid_new ,1,255)      as VARCHAR2(255))   as business_unit_address_guid,
        CAST(           A.business_unit_address_guid_old              as VARCHAR2(255))      as business_unit_address_guid_old,
        CAST( substr(   A.source_aisle_code                 ,1,60)    as VARCHAR(60))         as source_aisle_code,
        CAST( substr(   A.source_bin_code                   ,1,60)    as VARCHAR(60))         as source_bin_code,
        CAST( substr(   A.staging_location_flag             ,1,1)     as VARCHAR(1))          as staging_location_flag,
        CAST(           A.load_date                                   as TIMESTAMP_NTZ(9))    as load_date,
        CAST(           A.update_date                                 as TIMESTAMP_NTZ(9))    as update_date
    from hist_loc A
    LEFT JOIN UNIQUE_PASSTHROUGH B on A.unique_key = B.unique_key
    where b.source_system is null
),

Final as(
    select * from NEW_DIM
    UNION
    select * from OLD_DIM
)

select * from FINAL