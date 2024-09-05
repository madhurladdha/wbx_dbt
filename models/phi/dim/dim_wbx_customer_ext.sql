{{
    config(
    materialized = env_var('DBT_MAT_INCREMENTAL'),
    transient = false,
    unique_key = 'UNIQUE_KEY',
    on_schema_change='sync_all_columns',
    tags="rdm_core"
    )
}}

/*seq fields is being  hardcoded to 0 for data coming from ax world in old_dim cte below.
This has been alraedy been applied for data coming from D365
This is required to avoid duplicates in downstream model(stg_d_wbx_customer_planning) as seq number is also part of group by clause.
*/

WITH hist_customers as (
    select * from {{ref('conv_dim_wbx_customer_ext')}}
),
normalized_customers as (
    select 
        {{ dbt_utils.surrogate_key(['customer_address_number_guid']) }} AS UNIQUE_KEY, * from {{ref('stg_d_wbx_customer_ext')}}
), 

new_dim as (
select  
cast(substr(a.unique_key   ,1,255)  as text(255))      as unique_key,
cast(substr(a.customer_address_number_guid ,1,255)  as text(255))      as customer_address_number_guid,
CAST(B.CUSTOMER_ADDRESS_NUMBER_GUID_OLD AS NUMBER(38,0))  as CUSTOMER_ADDRESS_NUMBER_GUID_OLD,
cast(substr(a.company_address_guid   ,1,255)  as text(255))      as company_address_guid,
cast(substring(a.source_system,1,255) as text(255) ) as source_system  ,
cast(substring(a.source_system_address_number,1,255) as text(255) ) as source_system_address_number  ,
cast(substring(a.company_code,1,60) as text(60) ) as company_code  ,
cast(substring(a.market_code,1,60) as text(60) ) as market_code  ,
cast(substring(a.market_desc,1,255) as text(255) ) as market_desc  ,
cast(a.market_code_seq as number(38,0) ) as market_code_seq  ,
cast(substring(a.sub_market_code,1,60) as text(60) ) as sub_market_code  ,
cast(substring(a.sub_market_desc,1,255) as text(255) ) as sub_market_desc  ,
cast(a.sub_market_code_seq as number(38,0) ) as sub_market_code_seq  ,
cast(substring(a.trade_class_code,1,60) as text(60) ) as trade_class_code  ,
cast(substring(a.trade_class_desc,1,255) as text(255) ) as trade_class_desc  ,
cast(a.trade_class_seq as number(38,0) ) as trade_class_seq  ,
cast(substring(a.trade_group_code,1,60) as text(60) ) as trade_group_code  ,
cast(substring(a.trade_group_desc,1,255) as text(255) ) as trade_group_desc  ,
cast(a.trade_group_seq as number(38,0) ) as trade_group_seq  ,
cast(substring(a.trade_type_code,1,60) as text(60) ) as trade_type_code  ,
cast(substring(a.trade_type_desc,1,255) as text(255) ) as trade_type_desc  ,
cast(a.trade_type_seq as number(38,0) ) as trade_type_seq  ,
cast(substring(a.trade_sector_code,1,60) as text(60) ) as trade_sector_code  ,
cast(substring(a.trade_sector_desc,1,255) as text(255) ) as trade_sector_desc  ,
cast(a.trade_sector_seq as number(38,0) ) as trade_sector_seq  ,
cast(substring(a.price_group,1,60) as text(60) ) as price_group  ,
cast(substring(a.total_so_qty_discount,1,60) as text(60) ) as total_so_qty_discount  ,
cast(substring(a.additional_discount,1,60) as text(60) ) as additional_discount  ,
cast(substring(a.customer_rebate_group,1,60) as text(60) ) as customer_rebate_group  ,
cast(substring(a.currency,1,60) as text(60) ) as currency  ,
cast(substring(a.vat_group,1,60) as text(60) ) as vat_group  ,
cast(a.date_inserted as timestamp_ntz(9) ) as date_inserted  ,
cast(a.date_updated as timestamp_ntz(9) ) as date_updated  ,
cast(a.min_order_qty_ca as number(38,10) ) as min_order_qty_ca  ,
cast(a.min_order_qty_pallets as number(38,10) ) as min_order_qty_pallets  ,
cast(substring(a.full_pallet_flag,1,10) as text(10) ) as full_pallet_flag  ,
cast(a.max_order_qty_ca as number(38,10) ) as max_order_qty_ca  ,
cast(a.max_order_qty_pallets as number(38,10) ) as max_order_qty_pallets  ,
cast(substring(a.fin_dim_cost_centre,1,255) as text(255) ) as fin_dim_cost_centre  ,
cast(substring(a.fin_dim_customer,1,255) as text(255) ) as fin_dim_customer  ,
cast(substring(a.fin_dim_department,1,255) as text(255) ) as fin_dim_department  ,
cast(substring(a.fin_dim_site,1,255) as text(255) ) as fin_dim_site  
from normalized_customers A
LEFT JOIN hist_customers B
ON A.source_system_address_number = B.source_system_address_number and a.company_code = b.company_code
),


old_dim as 
(
select  
cast(substr(a.unique_key   ,1,255)  as text(255))      as unique_key,
cast(substr(a.customer_address_number_guid ,1,255)  as text(255))      as customer_address_number_guid,
CAST(a.CUSTOMER_ADDRESS_NUMBER_GUID_OLD AS NUMBER(38,0))  as CUSTOMER_ADDRESS_NUMBER_GUID_OLD,
cast(substr(a.company_address_guid   ,1,255)  as text(255))      as company_address_guid,
cast(substring(a.source_system,1,255) as text(255) ) as source_system  ,
cast(substring(a.source_system_address_number,1,255) as text(255) ) as source_system_address_number  ,
cast(substring(a.company_code,1,60) as text(60) ) as company_code  ,
cast(substring(a.market_code,1,60) as text(60) ) as market_code  ,
cast(substring(a.market_desc,1,255) as text(255) ) as market_desc  ,
cast(0 as number(38,0) ) as market_code_seq  , 
cast(substring(a.sub_market_code,1,60) as text(60) ) as sub_market_code  ,
cast(substring(a.sub_market_desc,1,255) as text(255) ) as sub_market_desc  ,
cast(0 as number(38,0) ) as sub_market_code_seq  ,
cast(substring(a.trade_class_code,1,60) as text(60) ) as trade_class_code  ,
cast(substring(a.trade_class_desc,1,255) as text(255) ) as trade_class_desc  ,
cast(0 as number(38,0) ) as trade_class_seq  ,
cast(substring(a.trade_group_code,1,60) as text(60) ) as trade_group_code  ,
cast(substring(a.trade_group_desc,1,255) as text(255) ) as trade_group_desc  ,
cast(0 as number(38,0) ) as trade_group_seq  ,
cast(substring(a.trade_type_code,1,60) as text(60) ) as trade_type_code  ,
cast(substring(a.trade_type_desc,1,255) as text(255) ) as trade_type_desc  ,
cast(0 as number(38,0) ) as trade_type_seq  ,
cast(substring(a.trade_sector_code,1,60) as text(60) ) as trade_sector_code  ,
cast(substring(a.trade_sector_desc,1,255) as text(255) ) as trade_sector_desc  ,
cast(0 as number(38,0) ) as trade_sector_seq  ,
cast(substring(a.price_group,1,60) as text(60) ) as price_group  ,
cast(substring(a.total_so_qty_discount,1,60) as text(60) ) as total_so_qty_discount  ,
cast(substring(a.additional_discount,1,60) as text(60) ) as additional_discount  ,
cast(substring(a.customer_rebate_group,1,60) as text(60) ) as customer_rebate_group  ,
cast(substring(a.currency,1,60) as text(60) ) as currency  ,
cast(substring(a.vat_group,1,60) as text(60) ) as vat_group  ,
cast(a.date_inserted as timestamp_ntz(9) ) as date_inserted  ,
cast(a.date_updated as timestamp_ntz(9) ) as date_updated  ,
cast(a.min_order_qty_ca as number(38,10) ) as min_order_qty_ca  ,
cast(a.min_order_qty_pallets as number(38,10) ) as min_order_qty_pallets  ,
cast(substring(a.full_pallet_flag,1,10) as text(10) ) as full_pallet_flag  ,
cast(a.max_order_qty_ca as number(38,10) ) as max_order_qty_ca  ,
cast(a.max_order_qty_pallets as number(38,10) ) as max_order_qty_pallets  ,
cast(substring(a.fin_dim_cost_centre,1,255) as text(255) ) as fin_dim_cost_centre  ,
cast(substring(a.fin_dim_customer,1,255) as text(255) ) as fin_dim_customer  ,
cast(substring(a.fin_dim_department,1,255) as text(255) ) as fin_dim_department  ,
cast(substring(a.fin_dim_site,1,255) as text(255) ) as fin_dim_site
from hist_customers A
Left JOIN normalized_customers B
ON A.source_system_address_number = B.source_system_address_number and a.company_code = b.company_code
WHERE B.source_system_address_number is NULL
),


Final_Dim 
as 
(
SELECT * FROM new_dim 
UNION 
SELECT * FROM old_dim
) 

 Select * from Final_Dim 