{{
    config(
    materialized =env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_dim"]
    )
}}

with old_dim as (
    select * from {{source('WBX_PROD','xref_wbx_payment_terms')}} where  {{env_var("DBT_PICK_FROM_CONV")}}='Y' /*adding variable to include/exclude conversion model data.if variable DBT_PICK_FROM_CONV has value 'Y' then conversion model will pull data from hist else it will be null */
),

payterm_cross_ref as (select * from {{ ref("payterm_d365_ref") }}),


converted_dim as (
    select
        
        SRC.PAYMENT_TERMS_GUID as PAYMENT_TERMS_GUID_OLD,
        SOURCE_SYSTEM,
        SRC.SOURCE_PAYMENT_TERMS_CODE as SOURCE_PAYMENT_TERMS_CODE_OLD,
        nvl(ref.d365,src.SOURCE_PAYMENT_TERMS_CODE) as SOURCE_PAYMENT_TERMS_CODE_NEW,
        {{ dbt_utils.surrogate_key(['SOURCE_SYSTEM','SOURCE_PAYMENT_TERMS_CODE_NEW']) }} AS PAYMENT_TERMS_GUID,
        PAYMENT_TERMS_CODE,
        PAYMENT_TERMS_DESCRIPTION,
        DAYS_TO_PAY,
        DAYS_TO_DISCOUNT,
        DISCOUNT_PERCENT,
        LOAD_DATE,
        UPDATE_DATE
    from old_dim SRC
    left join payterm_cross_ref ref on upper(trim(src.SOURCE_PAYMENT_TERMS_CODE))=upper(trim(ref.ax))
)

select         
{{ dbt_utils.surrogate_key(['payment_terms_guid']) }} as UNIQUE_KEY,* from converted_dim



