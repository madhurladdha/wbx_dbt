{{
    config(
    materialized = env_var('DBT_MAT_INCREMENTAL'),
    transient = false,
    unique_key = 'UNIQUE_KEY',
    on_schema_change='sync_all_columns'
    )
}}

with item AS 
(
    SELECT * FROM {{ref('int_d_wbx_item')}}
),

conv_item as
(
    select * from {{ref('conv_dim_wbx_sls_item_category')}} 
),

item_sales as 
(
    select
        {{ dbt_utils.surrogate_key(['item.item_guid','item.business_unit_address_guid']) }} as unique_key,
        source_system,
        source_item_identifier,
        null as item_guid_old,
        item_guid as item_guid,
        source_business_unit_code,
        null as business_unit_address_guid_old,
        business_unit_address_guid,
        /* fields for Sales Category */
		customer_selling_unit,
		sales_catergory1_code,
		sales_catergory2_code,
		sales_catergory3_code,
		sales_catergory4_code,
		sales_catergory5_code,
		cost_object,
		profit_loss_code,
		freight_handling,
		default_broker_comm_rate,
		consumer_unit_size,
		label_owner,
		manufacturer_id,
        systimestamp() as update_date,
        systimestamp() as load_date
    from item
),

new_dim as (

    select 
    a.unique_key,
	a.source_system,
	a.source_item_identifier,
	null as item_guid_old,
	a.item_guid,
	a.source_business_unit_code,
	null as business_unit_address_guid_old,
	a.business_unit_address_guid,

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
    from
    item_sales a left join conv_item b  on a.item_guid=b.item_guid and a.business_unit_address_guid=b.business_unit_address_guid

),

old_dim as 
(
    select 
    a.unique_key,
	a.source_system,
	a.source_item_identifier,
	null as item_guid_old,
	a.item_guid,
	a.source_business_unit_code,
	null as business_unit_address_guid_old,
	a.business_unit_address_guid,
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
    from
    conv_item a left join item_sales b  on a.item_guid=b.item_guid and a.business_unit_address_guid=b.business_unit_address_guid
	where b.business_unit_address_guid is null

),

final_dim as (

    select * from new_dim
    union 
    select * from old_dim
),


item_sales_with_cast as 
(
    select
        cast (substr(unique_key,1,255) as varchar2(255)) as unique_key,
        cast (substr(source_system,1,255) as varchar2(255)) as source_system,
        cast (substr(source_item_identifier,1,60) as varchar2(60)) as source_item_identifier,
        cast (item_guid_old as numeric(38,10)) as item_guid_old,
        cast (substr(item_guid,1,255) as varchar2(255)) as item_guid,
        cast (substr(source_business_unit_code,1,255) as varchar2(255)) as source_business_unit_code,
        cast (business_unit_address_guid_old as numeric(38,10)) as business_unit_address_guid_old,
        cast (substr(business_unit_address_guid,1,255) as varchar2(255)) as business_unit_address_guid,

		cast(substring(customer_selling_unit,1,60) as text(60) ) as customer_selling_unit,
		cast(substring(sales_catergory1_code,1,255) as text(255) ) as sales_catergory1_code,
		cast(substring(sales_catergory2_code,1,255) as text(255) ) as sales_catergory2_code,
		cast(substring(sales_catergory3_code,1,255) as text(255) ) as sales_catergory3_code,
		cast(substring(sales_catergory4_code,1,255) as text(255) ) as sales_catergory4_code,
		cast(substring(sales_catergory5_code,1,255) as text(255) ) as sales_catergory5_code,
		cast(substring(cost_object,1,60) as text(60) ) as cost_object,
		cast(substring(profit_loss_code,1,60) as text(60) ) as profit_loss_code,
		cast(substring(freight_handling,1,60) as text(60) ) as freight_handling,
		cast(substring(default_broker_comm_rate,1,60) as text(60) ) as default_broker_comm_rate,
		cast(substring(consumer_unit_size,1,60) as text(60) ) as consumer_unit_size,
		cast(substring(label_owner,1,60) as text(60) ) as label_owner,	
		cast(substring(manufacturer_id,1,60) as text(60) ) as manufacturer_id,
        NULL as dimension_group, -- adding as NULL as IICS table has NULL for wbx
        NULL as plcode_label_owner, -- adding as NULL as IICS table has NULL for wbx
        cast (update_date as timestamp_ntz(9)) as update_date,
        cast (load_date as timestamp_ntz(9)) as load_date

    from final_dim
)

select * from item_sales_with_cast