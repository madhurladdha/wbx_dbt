{{
  config( 
    materialized=env_var('DBT_MAT_INCREMENTAL'), 
    tags=["manufacturing", "manufacturing_plant", "manufacturing_plant_weekday"],
    )
}}

/*
This model is incremental. This looks up to itself to get version number.

in first load it will load historical data and then in subsequent runs it will be incremental 

*/


with cte_stage as 
(
    select * from {{ ref('stg_f_wbx_mfg_plant_wc_weekday_xref') }}
),
{% if check_table_exists( this.schema, this.table ) == 'True' %}
cte_vrsn_nbr as 
(
    select 
        version_date, 
        max(version_number) as version_number  
    from {{ this }} 
    group by version_date
),
cte_lkp_vrsn_nbr as 
(
    select 
        source_system,
        stg.version_date,
        source_business_unit_code,
        business_unit_address_guid,
        work_center_code,
        snapshot_day,
        effective_date,
        expiration_date,
        load_date,
        update_date,
        case 
            when nbr.version_number is null then 1
            when nbr.version_date = stg.version_date then nbr.version_number + 1
        end as version_number
    from cte_stage stg 
    left join cte_vrsn_nbr nbr 
    on stg.version_date = nbr.version_date
),
cte_incremental as 
(
    select 
            cast(substring(source_system,1,255) as text(255) ) as source_system  ,

            cast(version_date as date) as version_date  ,

            cast(version_number as number(20,0) ) as version_number  ,

            cast(substring(source_business_unit_code,1,255) as text(255) ) as source_business_unit_code  ,

            cast(business_unit_address_guid as text(255) ) as business_unit_address_guid  ,

            cast(substring(work_center_code,1,255) as text(255) ) as work_center_code  ,

            cast(substring(snapshot_day,1,255) as text(255) ) as snapshot_day  ,

            cast(effective_date as date) as effective_date  ,

            cast(expiration_date as date) as expiration_date  ,

            cast(load_date as timestamp_ntz(9) ) as load_date  ,

            cast(update_date as timestamp_ntz(9) ) as update_date 
    from cte_lkp_vrsn_nbr
),
{% endif %}
old_table as
(
    select * from {{ref('conv_fct_wbx_mfg_plant_wc_weekday_xref')}}  
    {% if check_table_exists( this.schema, this.table ) == 'False' %}
     limit {{env_var('DBT_NO_LIMIT')}} ----------Variable DBT_NO_LIMIT variable is set TO NULL to load everything from conv model if effective currency model is not present.
    {% else %} limit {{env_var('DBT_LIMIT')}}-----Variable DBT_LIMIT variable is set to 0 to load nothing if effective_currency table exist

{% endif %}
),
cte_final as 
(
    select * from old_table
    {% if check_table_exists( this.schema, this.table ) == 'True' %}
    union all
    select * from cte_incremental
    {% endif %}
)
select * from cte_final