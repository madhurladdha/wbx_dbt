{{
    config (
    materialized = env_var('DBT_MAT_TABLE'),
    tag=['ax_hist_dim']
    )
}}



with old_dim as (
select 'SUPPLIER' AS GENERIC_ADDRESS_TYPE,* from {{source('WBX_PROD','dim_wbx_prc_supplier_categorization')}} WHERE {{env_var("DBT_PICK_FROM_CONV")}}='Y' /*adding variable to include/exclude conversion model data.if variable DBT_PICK_FROM_CONV has value 'Y' then conversion model will pull data from hist else it will be null */
),

VENDTABLE as (
    select distinct ACCOUNTNUM,UPPER(DATAAREAID) as company_code from {{ source('WBX_PROD_SRC','SRC_VENDTABLE') }}
    union
    select distinct ACCOUNTNUM,UPPER(DATAAREAID) as company_code from {{ ref('src_vendtable') }}
),

converted_dim as (
select
{{ dbt_utils.surrogate_key(['SOURCE_SYSTEM','SOURCE_SYSTEM_ADDRESS_NUMBER','GENERIC_ADDRESS_TYPE','VENDTABLE.COMPANY_CODE']) }}  AS GENERATED_ADDRESS_NUMBER,
GENERATED_ADDRESS_NUMBER_OLD,
SOURCE_SYSTEM,
SOURCE_SYSTEM_ADDRESS_NUMBER,
SOURCE_NAME,
ADDRESS_LINE_1,
SUPPLIER_TYPE,
DATE_INSERTED,
DATE_UPDATED,
VENDTABLE.COMPANY_CODE as COMPANY_CODE
from old_dim left join VENDTABLE on VENDTABLE.ACCOUNTNUM=old_dim.SOURCE_SYSTEM_ADDRESS_NUMBER
)

select {{ dbt_utils.surrogate_key(['GENERATED_ADDRESS_NUMBER']) }}  AS unique_key,* from converted_dim