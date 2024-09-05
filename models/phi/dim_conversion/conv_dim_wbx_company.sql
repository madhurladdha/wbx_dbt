{{
    config(
    materialized =env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_dim"]
    )
}}

WITH old_dim AS 
(
    SELECT * FROM {{source('WBX_PROD','dim_wbx_company')}} WHERE  {{env_var("DBT_PICK_FROM_CONV")}}='Y'
),

converted_dim as (
    SELECT  
        UNIQUE_KEY,
        GENERIC_ADDRESS_TYPE,
        company_address_guid,
        company_address_guid_OLD,
        source_system,
        type,
        division,
        region,
        company_code,
        company_name,
        operating_company,
        segment,
        default_currency_code,
        date_inserted,
        date_updated,
        parent_currency_code
    FROM old_dim
)
Select * from converted_dim


