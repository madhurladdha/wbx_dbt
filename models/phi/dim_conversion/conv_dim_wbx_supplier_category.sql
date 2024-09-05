
{{
    config(
    materialized =env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_dim"]
    )
}}


with old_dim as (
    Select 'SUPPLIER' as GENERIC_ADDRESS_TYPE,* from
     {{source('WBX_PROD','dim_wbx_supplier_category')}} WHERE {{env_var("DBT_PICK_FROM_CONV")}}='Y' 
),

VENDTABLE as (
    select distinct ACCOUNTNUM,UPPER(DATAAREAID) as company_code from {{ source('WBX_PROD_SRC','SRC_VENDTABLE') }}
    union
    select distinct ACCOUNTNUM,UPPER(DATAAREAID) as company_code from {{ ref('src_vendtable') }}
),

converted_dim as (
    select
    SOURCE_SYSTEM,
    SOURCE_SYSTEM_ADDRESS_NUMBER,
   -- SUPPLIER_ADDRESS_NUMBER_GUID,
    GENERIC_ADDRESS_TYPE,
    SUPPLIER_NAME,
    UPDATE_DATE,
    UPDATED_BY,
    VENDTABLE.COMPANY_CODE as COMPANY_CODE
    from old_dim left join VENDTABLE on VENDTABLE.ACCOUNTNUM=old_dim.SOURCE_SYSTEM_ADDRESS_NUMBER
),

guid as (
select {{ dbt_utils.surrogate_key(['SOURCE_SYSTEM','SOURCE_SYSTEM_ADDRESS_NUMBER','GENERIC_ADDRESS_TYPE','COMPANY_CODE']) }} as SUPPLIER_ADDRESS_NUMBER_GUID,*
from converted_dim )

select {{ dbt_utils.surrogate_key(['SUPPLIER_ADDRESS_NUMBER_GUID']) }} as unique_key,
    SOURCE_SYSTEM,
    SOURCE_SYSTEM_ADDRESS_NUMBER,
    SUPPLIER_ADDRESS_NUMBER_GUID,
    SUPPLIER_NAME,
    UPDATE_DATE,
    UPDATED_BY,
    company_code
    from guid