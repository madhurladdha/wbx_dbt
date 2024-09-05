{{
    config(
    materialized =env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_dim"]
    )
}}


with old_dim as (
    select
        'PLANT_DC' as generic_address_type,
        *
    from {{ source('WBX_PROD','dim_wbx_sls_item_category') }}
),

old_plant as (
    select distinct
        source_system,
        source_business_unit_code,
        source_business_unit_code_new,
        plantdc_address_guid,
        plantdc_address_guid_new
    from {{ ref('conv_dim_wbx_plant_dc') }}
    where type != 'COST CENTER'

),

converted_dim as (
    select
        a.source_system,
        a.item_guid as item_guid_old,
        a.item_guid as item_guid,
        b.plantdc_address_guid as business_unit_address_guid_old,
        b.plantdc_address_guid_new as business_unit_address_guid,
        a.source_item_identifier,
        b.source_business_unit_code_new as source_business_unit_code,
        a.customer_selling_unit,
        a.sales_catergory1_code,
        a.sales_catergory2_code,
        a.sales_catergory3_code,
        a.sales_catergory4_code,
        a.sales_catergory5_code,
        a.cost_object,
        a.profit_loss_code,
        a.freight_handling,
        a.default_broker_comm_rate,
        a.consumer_unit_size,
        a.label_owner,
        a.manufacturer_id,
        a.update_date,
        a.load_date
    from old_dim as a
    inner join old_plant as b
        on
            a.source_system = b.source_system
            and a.business_unit_address_guid = b.plantdc_address_guid
)

select
    {{ dbt_utils.surrogate_key(['item_guid','business_unit_address_guid']) }}
        as unique_key,
    * from converted_dim QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_key ORDER BY source_business_unit_code) = 1