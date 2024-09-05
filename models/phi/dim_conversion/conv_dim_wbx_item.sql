{{
    config(
    materialized = env_var('DBT_MAT_TABLE'),
    tag=['ax_hist_dim']
    )
}}

WITH old_dim AS 
(
    SELECT * FROM {{source('WBX_PROD','dim_wbx_item')}} 
),


old_plant as
(
 select distinct
        source_system, 
        source_business_unit_code_new,
        source_business_unit_code,
        plantdc_address_guid,
        plantdc_address_guid_NEW,
        'PLANT_DC' as PLANT_GENERIC_ADDRESS_TYPE
    from {{ ref('conv_dim_wbx_plant_dc') }}
),

converted_dim as (
    select distinct
        a.source_system,
        null as item_guid_old,
        item_guid,
        a.business_unit_address_guid as business_unit_address_guid_old,
        b.plantdc_address_guid_NEW as business_unit_address_guid,
        a.source_item_identifier,
        b.source_business_unit_code_new as source_business_unit_code,
        a.case_item_number,
        a.legacy_case_item_number,
        a.description,
        a.pack_size_desc,
        a.short_description,
        a.bus_unit_desc,
        a.item_type,
        a.primary_uom,
        a.primary_uom_desc,
        a.SHELF_LIFE_DAYS,
        a.case_net_weight,
        a.case_gross_weight,
        a.alternate_item_number,
        a.item_class,
        a.obsolete_flag,
        a.MIN_REORDER_QUANTITY,
        a.MAX_REORDER_QUANTITY,
        a.DIVISION,
        a.DIVISION_C0DE,
        a.STOCK_TYPE,
        a.STOCK_DESC,
        a.buyer_code,
		a.case_upc,
		a.purchase_make_indicator,
		a.planner_code,
		a.gl_class_name,
		a.consumer_gtin_number,
		a.formula_variation,
		a.multiple_order_quantity,
		a.reorder_point,
		a.reorder_quantity,
        a.vendor_address_guid, --this is not conventional guid value, it's actually supplier_code. In IICS world named as GUID 
        a.load_date,
        a.update_date,
        a.CONSUMER_UNIT_SIZE
    from old_dim a
    join old_plant b
          on  a.source_system = b.source_system 
          and a.business_unit_address_guid = b.plantdc_address_guid

)

Select 
    {{ dbt_utils.surrogate_key(['item_guid','business_unit_address_guid']) }} as unique_key,
    * 
from converted_dim qualify ROW_NUMBER() OVER(PARTITION BY UNIQUE_KEY ORDER BY 1)=1