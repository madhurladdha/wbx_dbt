{{
    config(
    on_schema_change='sync_all_columns',
    tags = "rdm_core"
    )
}}

with STG as (
    select * from {{ref('stg_d_wbx_work_center')}}
),

hist as(
    select * from {{ref('conv_adr_wtx_work_center')}} where 1=2
),

Final as(
select 
COMPANY_CODE,
SOURCE_BUSINESS_UNIT_CODE,
WORK_CENTER_CODE,
DESCRIPTION,
UPDATE_DATE,
WC_CATEGORY_DESC,
WC_CATEGORY_CODE,
SOURCE_SYSTEM
from stg
)

select * from final


