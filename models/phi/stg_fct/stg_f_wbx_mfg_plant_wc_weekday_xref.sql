{{
    config(
        tags=["manufacturing", "manufacturing_plant", "manufacturing_plant_weekday"],
    )
}}

with
    cte_source as (
        select
            source_business_unit_code,
            work_center_code,
            snapshot_day,
            effective_date,
            expiration_date,
            load_date,
            update_date,
            error_flag,
            error_msg,
            to_date(substr(convert_timezone('UTC', current_timestamp()), 1, 10)) as version_date,
            '{{env_var('DBT_SOURCE_SYSTEM')}}' as source_system
        from {{ ref("stg_f_wbx_mfg_plant_wc_weekday") }}

    )
    select 
        source_system,
        version_date,
        source_business_unit_code,
        {{ dbt_utils.surrogate_key(['source_system','source_business_unit_code',"'PLANT_DC'"]) }} as business_unit_address_guid,
        work_center_code,
        snapshot_day,
        effective_date,
        expiration_date,
        systimestamp() as load_date,
        systimestamp() as update_date
    from cte_source

