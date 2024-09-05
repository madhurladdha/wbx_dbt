{{
    config(

    materialized = env_var('DBT_MAT_INCREMENTAL'),
    transient = false,
    tags = "rdm_core",
    unique_key = 'UNIQUE_KEY',
    on_schema_change='sync_all_columns'
    )
}}

WITH HIST_ITEM as (
    select * from {{ref('conv_dim_wbx_item')}}
),

NORMALIZED_ITEM as (
 select {{ dbt_utils.surrogate_key(['ITEM_GUID','BUSINESS_UNIT_ADDRESS_GUID']) }} AS UNIQUE_KEY,* from {{ref('int_d_wbx_item')}}
),


A as(
SELECT 
SOURCE_SYSTEM,
SOURCE_ITEM_IDENTIFIER, 
SOURCE_BUSINESS_UNIT_CODE,
ALTERNATE_ITEM_NUMBER, 
DESCRIPTION, 
SHORT_DESCRIPTION, 
PACK_SIZE_DESC, 
DIVISION, 
SHELF_LIFE_DAYS
FROM 
{% if check_table_exists( this.schema, 'dim_wbx_item' ) == 'True' %}
{{ this }}
{% elif check_table_exists( this.schema, 'dim_wbx_item' ) != 'True' %}
HIST_ITEM
{% endif %}
),

STALE_ITEMS as (
SELECT 
a.SOURCE_SYSTEM, 
a.SOURCE_ITEM_IDENTIFIER, 
a.SOURCE_BUSINESS_UNIT_CODE,
a.ALTERNATE_ITEM_NUMBER, 
a.DESCRIPTION, 
a.SHORT_DESCRIPTION, 
a.PACK_SIZE_DESC, 
a.DIVISION, 
a.SHELF_LIFE_DAYS
FROM A
LEFT JOIN NORMALIZED_ITEM B ON A.SOURCE_ITEM_IDENTIFIER = B.SOURCE_ITEM_IDENTIFIER AND A.SOURCE_BUSINESS_UNIT_CODE = B.SOURCE_BUSINESS_UNIT_CODE
WHERE B.SOURCE_system IS NULL),

ITEM_ATTR as(
SELECT 
A.SOURCE_SYSTEM, 
A.SOURCE_ITEM_IDENTIFIER,
MAX(A.ALTERNATE_ITEM_NUMBER) AS ALTERNATE_ITEM_NUMBER,
MAX(A.DESCRIPTION) AS DESCRIPTION, 
MAX(A.SHORT_DESCRIPTION) AS SHORT_DESCRIPTION,
MAX(A.PACK_SIZE_DESC) AS PACK_SIZE_DESC,
MAX(A.DIVISION) AS DIVISION, 
MAX(A.SHELF_LIFE_DAYS) AS SHELF_LIFE_DAYS, 
MAX(A.STOCK_TYPE) AS STOCK_TYPE, 
MAX(A.STOCK_DESC) AS STOCK_DESC, 
MAX(A.ITEM_TYPE) AS ITEM_TYPE, 
MAX(A.ITEM_CLASS) AS ITEM_CLASS,
MAX(A.PRIMARY_UOM) AS PRIMARY_UOM, 
MAX(A.CASE_GROSS_WEIGHT) AS CASE_GROSS_WEIGHT, 
MAX(A.CASE_NET_WEIGHT) AS CASE_NET_WEIGHT, 
MAX(A.OBSOLETE_FLAG) AS OBSOLETE_FLAG, 
MAX(A.MAX_REORDER_QUANTITY) AS MAX_REORDER_QUANTITY,
MAX(A.MIN_REORDER_QUANTITY) AS MIN_REORDER_QUANTITY
FROM
{% if check_table_exists( this.schema, 'dim_wbx_item' ) == 'True' %}
{{ this }} A
{% elif check_table_exists( this.schema, 'dim_wbx_item' ) != 'True' %}
HIST_ITEM A
{% endif %}
where A.UPDATE_DATE = (
SELECT MAX(B.UPDATE_DATE) FROM 
{% if check_table_exists( this.schema, 'dim_wbx_item' ) == 'True' %}
{{ this }} B
{% elif check_table_exists( this.schema, 'dim_wbx_item' ) != 'True' %}
HIST_ITEM B 
{% endif %}
WHERE B.SOURCE_SYSTEM = 'WEETABIX' AND B.SOURCE_ITEM_IDENTIFIER = A.SOURCE_ITEM_IDENTIFIER )
GROUP BY A.SOURCE_SYSTEM, A.SOURCE_ITEM_IDENTIFIER 
),

itm_update as(
SELECT ITEM_ATTR.SOURCE_SYSTEM AS SOURCE_SYSTEM
,ITEM_ATTR.SOURCE_ITEM_IDENTIFIER AS SOURCE_ITEM_IDENTIFIER
,STALE_ITEMS.SOURCE_BUSINESS_UNIT_CODE AS SOURCE_BUSINESS_UNIT_CODE
,cast(substring(ITEM_ATTR.ALTERNATE_ITEM_NUMBER,1,60) as varchar2(60)) AS ALTERNATE_ITEM_NUMBER
,cast (substring(ITEM_ATTR.description,1,60) as varchar2(60)) as description
,cast (substring(ITEM_ATTR.short_description,1,60) as varchar2(60)) as short_description
,cast (substring(ITEM_ATTR.PACK_SIZE_DESC,1,255) as varchar2(255)) as PACK_SIZE_DESC
,cast(substring(ITEM_ATTR.DIVISION,1,255) as text(255) ) as division  
,cast(ITEM_ATTR.SHELF_LIFE_DAYS as number(38,10)) as shelf_life_days
,ITEM_ATTR.STOCK_TYPE AS STOCK_TYPE
,cast (substring(ITEM_ATTR.STOCK_DESC,1,60) as varchar2(60)) as STOCK_DESC
,cast (substring(ITEM_ATTR.ITEM_TYPE,1,255) as varchar2(255)) as item_type
,cast (substring(item_class,1,255) as varchar2(255)) as item_class
,cast (substring(ITEM_ATTR.PRIMARY_UOM,1,255) as varchar2(255)) as primary_uom
,cast(ITEM_ATTR.case_net_weight as number(38,10) ) as case_net_weight  
,cast(ITEM_ATTR.case_gross_weight as number(38,10) ) as case_gross_weight
,ITEM_ATTR.OBSOLETE_FLAG AS OBSOLETE_FLAG
,cast (substring(ITEM_ATTR.MAX_REORDER_QUANTITY,1,60) as varchar2(60)) as MAX_REORDER_QUANTITY
,cast (substring(ITEM_ATTR.MIN_REORDER_QUANTITY,1,60) as varchar2(60)) as MIN_REORDER_QUANTITY
from  ITEM_ATTR
inner join STALE_ITEMS ON ITEM_ATTR.SOURCE_SYSTEM = STALE_ITEMS.SOURCE_SYSTEM
AND ITEM_ATTR.SOURCE_ITEM_IDENTIFIER = STALE_ITEMS.SOURCE_ITEM_IDENTIFIER
),

new_dim as (
select
        cast (substring(a.unique_key,1,255) as varchar2(255)) as unique_key,
        cast (substring(a.source_system,1,255) as varchar2(255)) as source_system,
        cast (substring(a.source_item_identifier,1,60) as varchar2(60)) as source_item_identifier,
        cast (substring(a.item_guid,1,255) as varchar2(255)) as item_guid,
        CAST(nvl(A.item_guid_old,c.item_guid_old) as varchar2(255)) as item_guid_old,
        cast (substring(a.source_business_unit_code,1,60) as varchar2(60)) as source_business_unit_code,
        cast (substring(a.business_unit_address_guid,1,255) as varchar2(255)) as business_unit_address_guid,
        CAST(nvl(A.business_unit_address_guid_old,C.business_unit_address_guid_old) AS varchar2(255)) as business_unit_address_guid_old,
        cast (substring(a.BUS_UNIT_DESC,1,255) as varchar2(255)) as BUS_UNIT_DESC,
        cast (substring(a.case_item_number,1,60) as varchar2(60)) as case_item_number,
        cast (substring(a.legacy_case_item_number,1,60) as varchar2(60)) as legacy_case_item_number,
        cast (substring(nvl(a.description,b.description),1,60) as varchar2(60)) as description,
        cast (substring(nvl(a.short_description,b.short_description),1,60) as varchar2(60)) as short_description,
        cast (substring(a.item_type,1,255) as varchar2(255)) as item_type,
		cast (substring(a.item_class,1,255) as varchar2(255)) as item_class,
		cast(substring(a.obsolete_flag,1,1) as text(1) ) as obsolete_flag  ,
        cast(substring(a.division_c0de,1,255) as text(255) ) as division_c0de  ,
        cast(substring(nvl(a.division,b.division),1,255) as text(255) ) as division  ,
        cast (substring(nvl(a.primary_uom,b.primary_uom),1,255) as varchar2(255)) as primary_uom,
        cast (substring(a.primary_uom_desc,1,255) as varchar2(255)) as primary_uom_desc,
        cast (substring(nvl(a.PACK_SIZE_DESC,b.PACK_SIZE_DESC),1,255) as varchar2(255)) as PACK_SIZE_DESC,
        cast (substring(nvl(a.STOCK_TYPE,b.STOCK_TYPE),1,255) as varchar2(255)) as STOCK_TYPE,
        cast (substring(nvl(a.STOCK_DESC,b.STOCK_DESC),1,60) as varchar2(60)) as STOCK_DESC,
        cast (substring(a.buyer_code,1,60) as varchar2(60)) as buyer_code,
        cast(nvl(a.shelf_life_days,b.shelf_life_days) as number(38,10) ) as shelf_life_days,
        cast(nvl(a.case_net_weight,b.case_net_weight) as number(38,10) ) as case_net_weight,   
        cast(nvl(a.case_gross_weight,b.case_gross_weight) as number(38,10) ) as case_gross_weight,
        cast (substring(nvl(a.alternate_item_number,b.alternate_item_number),1,60) as varchar2(60)) as alternate_item_number,
        cast (substring(nvl(a.MAX_REORDER_QUANTITY,B.MAX_REORDER_QUANTITY),1,60) as varchar2(60)) as MAX_REORDER_QUANTITY,
        cast (substring(nvl(a.MIN_REORDER_QUANTITY,b.MIN_REORDER_QUANTITY),1,60) as varchar2(60)) as MIN_REORDER_QUANTITY,
        cast(substring(a.case_upc,1,60) as text(60) ) as case_upc,
		cast(substring(a.purchase_make_indicator,1,255) as text(255) ) as purchase_make_indicator,	
		cast(substring(a.planner_code,1,60) as text(60) ) as planner_code,	
		cast(substring(a.gl_class_name,1,60) as text(60) ) as gl_class_name,	
		cast(a.consumer_gtin_number as number(38,10) ) as consumer_gtin_number,			
		cast(substring(a.formula_variation,1,60) as text(60) ) as formula_variation,	
		cast(substring(a.multiple_order_quantity,1,60) as text(60) ) as multiple_order_quantity, 
		cast(substring(a.reorder_point,1,60) as text(60) ) as reorder_point,
		cast(substring(a.reorder_quantity,1,60) as text(60) ) as reorder_quantity,
        cast (a.vendor_address_guid as number(38,10)) as vendor_address_guid, --this is not conventional guid value, it's actually supplier_code. In IICS world named as GUID 
        cast(substring(a.CONSUMER_UNIT_SIZE,1,60) as text(60) ) as CONSUMER_UNIT_SIZE,
        NULL as pasta_shape_variant,
        current_timestamp as load_date,
        current_timestamp as update_date,
        cast('N' as text(1) ) as CONV_STATUS
		from NORMALIZED_ITEM A
        left join itm_update B on a.SOURCE_BUSINESS_UNIT_CODE = b.SOURCE_BUSINESS_UNIT_CODE and a.SOURCE_ITEM_IDENTIFIER = b.SOURCE_ITEM_IDENTIFIER
        LEFT JOIN HIST_ITEM C on  A.business_unit_address_guid=c.business_unit_address_guid and A.item_guid=c.item_guid

),


old_dim as (
select
        cast (substring(a.unique_key,1,255) as varchar2(255)) as unique_key,
        cast (substring(a.source_system,1,255) as varchar2(255)) as source_system,
        cast (substring(a.source_item_identifier,1,60) as varchar2(60)) as source_item_identifier,
        cast (substring(a.item_guid,1,255) as varchar2(255)) as item_guid,
        cast (a.item_guid_old as varchar2(255)) as item_guid_old,
        cast (substring(a.source_business_unit_code,1,60) as varchar2(60)) as source_business_unit_code,
        cast (substring(a.business_unit_address_guid,1,255) as varchar2(255)) as business_unit_address_guid,
        cast (a.business_unit_address_guid_old as varchar2(255)) as business_unit_address_guid_old,
        cast (substring(a.BUS_UNIT_DESC,1,255) as varchar2(255)) as BUS_UNIT_DESC,
        cast (substring(a.case_item_number,1,60) as varchar2(60)) as case_item_number,
        cast (substring(a.legacy_case_item_number,1,60) as varchar2(60)) as legacy_case_item_number,
        cast (substring(a.description,1,60) as varchar2(60)) as description,
        cast (substring(a.short_description,1,60) as varchar2(60)) as short_description,
        cast (substring(a.item_type,1,255) as varchar2(255)) as item_type,
		cast (substring(a.item_class,1,255) as varchar2(255)) as item_class,
		cast(substring(a.obsolete_flag,1,1) as text(1) ) as obsolete_flag  ,
        cast(substring(a.division_c0de,1,255) as text(255) ) as division_c0de  ,
        cast(substring(a.division,1,255) as text(255) ) as division  ,
        cast (substring(a.primary_uom,1,255) as varchar2(255)) as primary_uom,
        cast (substring(a.primary_uom_desc,1,255) as varchar2(255)) as primary_uom_desc,
        cast (substring(a.PACK_SIZE_DESC,1,255) as varchar2(255)) as PACK_SIZE_DESC,
        cast (substring(a.STOCK_TYPE,1,255) as varchar2(255)) as STOCK_TYPE,
        cast (substring(a.STOCK_DESC,1,60) as varchar2(60)) as STOCK_DESC,
        cast (substring(a.buyer_code,1,60) as varchar2(60)) as buyer_code,
        cast(a.shelf_life_days as number(38,10) ) as shelf_life_days,
        cast(a.case_net_weight as number(38,10) ) as case_net_weight,
        cast(a.case_gross_weight as number(38,10) ) as case_gross_weight,
        cast (substring(a.alternate_item_number,1,60) as varchar2(60)) as alternate_item_number,
        cast (substring(a.MAX_REORDER_QUANTITY,1,60) as varchar2(60)) as MAX_REORDER_QUANTITY,
        cast (substring(a.MIN_REORDER_QUANTITY,1,60) as varchar2(60)) as MIN_REORDER_QUANTITY,
        cast(substring(a.case_upc,1,60) as text(60) ) as case_upc,
		cast(substring(a.purchase_make_indicator,1,255) as text(255) ) as purchase_make_indicator,	
		cast(substring(a.planner_code,1,60) as text(60) ) as planner_code,	
		cast(substring(a.gl_class_name,1,60) as text(60) ) as gl_class_name,	
		cast(a.consumer_gtin_number as number(38,10) ) as consumer_gtin_number,			
		cast(substring(a.formula_variation,1,60) as text(60) ) as formula_variation,	
		cast(substring(a.multiple_order_quantity,1,60) as text(60) ) as multiple_order_quantity, 
		cast(substring(a.reorder_point,1,60) as text(60) ) as reorder_point,
		cast(substring(a.reorder_quantity,1,60) as text(60) ) as reorder_quantity,
        cast (a.vendor_address_guid as number(38,10)) as vendor_address_guid, --this is not conventional guid value, it's actually supplier_code. In IICS world named as GUID 
        cast(substring(a.CONSUMER_UNIT_SIZE,1,60) as text(60) ) as CONSUMER_UNIT_SIZE,
        NULL as pasta_shape_variant,
        cast (a.load_date as timestamp_ntz) as load_date,
        cast (a.update_date as timestamp_ntz) as update_date,
		cast('Y' as text(1) ) as CONV_STATUS
        from HIST_ITEM A
        LEFT JOIN NORMALIZED_ITEM B
        on  A.business_unit_address_guid=B.business_unit_address_guid and A.item_guid=B.item_guid
		where B.item_guid is null and B.business_unit_address_guid is null

),


Final_Dim 
as 
(
SELECT * FROM new_dim 
UNION 
SELECT * FROM old_dim
) 

 Select * from Final_Dim
