  {{
    config(
     materialized =env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_dim"]
    )
}}

WITH old_dim AS 
(
    SELECT * FROM {{source('WBX_PROD','dim_wbx_location')}} WHERE  {{env_var("DBT_PICK_FROM_CONV")}}='Y'
),

old_plant as(
    select * from {{ref('conv_dim_wbx_plant_dc')}}
),

conv_dim as 
(
    Select
        source_location_code,
        A.source_business_unit_code as source_business_unit_code,
        b.source_business_unit_code_NEW as SOURCE_BUSINESS_UNIT_CODE_NEW,
        A.source_system as source_system,
        A.location_guid as location_guid_old,
        {{ dbt_utils.surrogate_key(['A.SOURCE_SYSTEM','A.SOURCE_LOCATION_CODE','B.SOURCE_BUSINESS_UNIT_CODE_NEW']) }} as location_guid,
        A.business_unit_address_guid as business_unit_address_guid_old,
        b.PLANTDC_ADDRESS_GUID_NEW as business_unit_address_guid_NEW,
        source_aisle_code,
        source_bin_code,
        staging_location_flag,
        A.load_date as load_date,
        A.update_date as update_date
    from old_dim A
    left join old_plant B  on
    A.SOURCE_BUSINESS_UNIT_CODE = coalesce(trim(B.SOURCE_BUSINESS_UNIT_CODE),'-')
)

select {{ dbt_utils.surrogate_key(['location_guid']) }} as UNIQUE_KEY,
* from conv_dim QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_key ORDER BY source_business_unit_code) = 1

/*using qualify to bring only one record in case of same unique_key with different old source_business_unit_code  ex. "unique_key='5eaf686f4c0942de8718242a87163787'"*/