{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        tags=env_var("DBT_TAGS"),
        transient=false,
        unique_key="UNIQUE_KEY",
        on_schema_change="sync_all_columns"
    )
}}

with src as (
select  
        RTRIM(LTRIM(Plant)) as SOURCE_BUSINESS_UNIT_CODE,
        RTRIM(LTRIM(Work_Center_Name)) as WORK_CENTER_CODE,
        case when REGEXP_COUNT(ctp_target, '^[0-9]+$') = 1 then CTP_Target else '0' end as CTP_Target,
        case when REGEXP_COUNT(ptp_target, '^[0-9]+$') = 1 then CTP_Target else '0' end as ptp_target,
        Systimestamp() as LOAD_DATE,
        Systimestamp() as update_date,
        {{ dbt_utils.surrogate_key(['SOURCE_BUSINESS_UNIT_CODE','WORK_CENTER_CODE']) }} as unique_key
from {{ ref("src_ctp_plant_wc_target") }}
)
, dim_wbx_plant_dc as  (
SELECT
        PLANTDC_ADDRESS_GUID AS PLANTDC_ADDRESS_GUID,
        SOURCE_SYSTEM AS SOURCE_SYSTEM,
        SOURCE_BUSINESS_UNIT_CODE AS SOURCE_BUSINESS_UNIT_CODE,
        ROW_NUMBER() OVER (PARTITION BY SOURCE_BUSINESS_UNIT_CODE,SOURCE_SYSTEM ORDER BY 1) rowNum
FROM {{ ref('dim_wbx_plant_dc') }}
WHERE SOURCE_SYSTEM = '{{ env_var("DBT_SOURCE_SYSTEM") }}' 
)
select
    cast(substring(src.source_business_unit_code,1,255) as text(255) ) as source_business_unit_code  ,

    cast(PLANTDC_ADDRESS_GUID as text(255) ) as business_unit_address_guid  ,

    cast(substring(src.work_center_code,1,255) as text(255) ) as work_center_code  ,

    cast(src.ctp_target as number(20,0) ) as ctp_target  ,

    cast(src.ptp_target as number(20,0) ) as ptp_target  ,

    cast(src.load_date as timestamp_ntz(9) ) as load_date  ,

    cast(src.update_date as timestamp_ntz(9) ) as update_date,

    cast(src.unique_key as text(255) ) as unique_key
from src
left join dim_wbx_plant_dc
on dim_wbx_plant_dc.SOURCE_SYSTEM = '{{ env_var("DBT_SOURCE_SYSTEM") }}'
and dim_wbx_plant_dc.SOURCE_BUSINESS_UNIT_CODE = src.SOURCE_BUSINESS_UNIT_CODE
and rowNum = 1