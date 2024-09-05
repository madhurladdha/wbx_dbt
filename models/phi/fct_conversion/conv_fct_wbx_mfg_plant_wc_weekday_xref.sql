{{
    config(
    materialized = env_var('DBT_MAT_VIEW'),
    )
}}

WITH old_fct AS 
(
    SELECT * FROM {{source('FACTS_FOR_COMPARE','mfg_wtx_plant_wc_weekday_xref')}} where  {{env_var("DBT_PICK_FROM_CONV")}}='Y'
)
    SELECT  
           cast(substring(source_system,1,255) as text(255) ) as source_system  ,

            cast(version_date as date) as version_date  ,

            cast(version_number as number(20,0) ) as version_number  ,

            cast(substring(source_business_unit_code,1,255) as text(255) ) as source_business_unit_code  ,

            cast({{ dbt_utils.surrogate_key(['source_system','source_business_unit_code',"'PLANT_DC'"]) }} as text(255) ) as business_unit_address_guid  ,

            cast(substring(work_center_code,1,255) as text(255) ) as work_center_code  ,

            cast(substring(snapshot_day,1,255) as text(255) ) as snapshot_day  ,

            cast(effective_date as date) as effective_date  ,

            cast(expiration_date as date) as expiration_date  ,

            cast(load_date as timestamp_ntz(9) ) as load_date  ,

            cast(update_date as timestamp_ntz(9) ) as update_date 
    FROM old_fct



