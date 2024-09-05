{{
    config(
    materialized = env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_dim"]
    )
}}

with old_dim as  (
    select * from {{ source('WBX_PROD','dim_wbx_lot') }} WHERE  {{env_var("DBT_PICK_FROM_CONV")}}='Y'
),

old_plant as (
    select * from {{ref('conv_dim_wbx_plant_dc')}}
), /*using this cte to bring new source business unit code */

conv_dim as (
    select
        SOURCE_LOT_CODE,
        SOURCE_ITEM_IDENTIFIER,
        A.SOURCE_BUSINESS_UNIT_CODE,
        B.SOURCE_BUSINESS_UNIT_CODE_NEW,
        A.SOURCE_SYSTEM,
        LOT_DESC,
        LOT_STATUS_CODE,
        LOT_STATUS_DESC,
        QUARANTINE_FLAG,
        SUPPLIER_LOT_CODE,
        SOURCE_SUPPLIER_CODE_PRIMARY,
        LOT_EXPIRATION_DATE,
        LOT_ONHAND_DATE,
        LOT_SELLBY_DATE,
        LOT_AGE_DAYS,
        LOT_EXPIRED_FLAG,
        A.LOAD_DATE,
        A.UPDATE_DATE,
        A.LOT_GUID as LOT_GUID_OLD,
        A.ITEM_GUID as ITEM_GUID_OLD,
        A.BUSINESS_UNIT_ADDRESS_GUID as BUSINESS_UNIT_ADDRESS_GUID_OLD,
        PLANTDC_ADDRESS_GUID_NEW as BUSINESS_UNIT_ADDRESS_GUID,
        ITEM_GUID,
        {{ dbt_utils.surrogate_key(['A.SOURCE_SYSTEM','B.SOURCE_BUSINESS_UNIT_CODE_NEW','A.SOURCE_ITEM_IDENTIFIER','A.SOURCE_LOT_CODE']) }} AS LOT_GUID,
    from old_dim A
    left join old_plant B
    ON A.SOURCE_BUSINESS_UNIT_CODE = coalesce(trim(B.SOURCE_BUSINESS_UNIT_CODE),'-')
)

select {{ dbt_utils.surrogate_key(['LOT_GUID','ITEM_GUID','BUSINESS_UNIT_ADDRESS_GUID'])}} AS UNIQUE_KEY,* from conv_dim 