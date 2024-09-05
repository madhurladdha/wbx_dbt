{{
    config(
    materialized =env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_dim"]
    )
}}

with old_dim as (
    select * from {{ source('WBX_PROD','dim_wbx_supplier') }} WHERE {{env_var("DBT_PICK_FROM_CONV")}}='Y'
),

VENDTABLE as (select distinct ACCOUNTNUM,UPPER(DATAAREAID) as company_code from {{ source('WBX_PROD_SRC','SRC_VENDTABLE') }}),

converted_dim as (
select distinct
SUPPLIER_ADDRESS_NUMBER_GUID_OLD,
SOURCE_SYSTEM_ADDRESS_NUMBER,
GENERIC_ADDRESS_TYPE,
PAYMENT_INSTRUMENT_CODE,
PAYMENT_INSTRUMENT_NAME,
UNIFIED_SUPPLIER_NAME,
SOURCE_SUPPLIER_TYPE,
PHI_SUPPLIER_SUBTYPE,
PAYMENT_TERMS_CODE,
PAYMENT_TERMS_DESCRIPTION,
PHI_SUPPLIER_TYPE,
TRANSPORT_MODE,
CURRENCY_CODE,
SHIPPING_TERMS,
SOURCE_SYSTEM,
SUPPLIER_NAME,
DATE_INSERTED,
DATE_UPDATED,
VOUCHER_DATE,
nvl(VENDTABLE.COMPANY_CODE,'WBX') as COMPANY_CODE
    from old_dim left join VENDTABLE on VENDTABLE.ACCOUNTNUM=old_dim.SOURCE_SYSTEM_ADDRESS_NUMBER
),

guid as (select {{ dbt_utils.surrogate_key(['SOURCE_SYSTEM','SOURCE_SYSTEM_ADDRESS_NUMBER','GENERIC_ADDRESS_TYPE','COMPANY_CODE']) }} as SUPPLIER_ADDRESS_NUMBER_GUID,* 
from converted_dim )

select {{ dbt_utils.surrogate_key(['SUPPLIER_ADDRESS_NUMBER_GUID']) }} AS UNIQUE_KEY,
{{ dbt_utils.surrogate_key(['SOURCE_SYSTEM','COMPANY_CODE',"'COMPANY'"]) }} as COMPANY_ADDRESS_GUID,
* from guid
WHERE (SOURCE_SYSTEM_ADDRESS_NUMBER||COMPANY_CODE <> '9900000RFL')
/* This code is added as it was creating cartesian join so we have removed the records for RFL as the same filter was in Stg model for supplier in AX*/