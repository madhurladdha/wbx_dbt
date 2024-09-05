{{ config(
    tags=["manufacturing", "cbom","mfg_cbom","sales","cogs","pcos"]
) }}
with source as (
    select * from {{ ref('stg_f_wbx_mfg_cbom')}}
),
final as (
    select 
        source_system	            as source_system,
        active_flag	                as active_flag,
        version_id	                as version_id,
        eff_date	                as eff_date,
        creation_date_time	        as creation_date_time,
        expir_date	                as expir_date,
        source_updated_datetime	    as source_updated_datetime,
        transaction_currency	    as transaction_currency,
        transaction_uom	            as transaction_uom,
        root_company_code	        as root_company_code,
        root_src_item_identifier	as root_src_item_identifier,
        root_src_variant_code	    as root_src_variant_code,
        {{dbt_utils.surrogate_key(
         ["src.source_system",
          "src.root_src_item_identifier",
          "source_business_unit_code",
         ])
        }}                          as root_src_item_guid,
        comp_src_item_identifier	as comp_src_item_identifier,
        comp_src_variant_code	    as comp_src_variant_code,
        {{dbt_utils.surrogate_key(
         ["src.source_system",
          "src.comp_src_item_identifier",
          "source_business_unit_code",
         ])
        }}	                        as comp_src_item_guid,
        comp_consumption_qty	    as comp_consumption_qty,
        comp_consumption_unit	    as comp_consumption_unit,
        comp_cost_price	            as comp_cost_price,
        comp_cost_price_unit	    as comp_cost_price_unit,
        comp_item_unit_cost	        as comp_item_unit_cost,
        comp_bom_level	            as comp_bom_level,
        comp_calctype_desc	        as comp_calctype_desc,
        comp_cost_group_id	        as comp_cost_group_id,
        parent_item_indicator	    as parent_item_indicator,
        source_business_unit_code	as source_business_unit_code,
        source_bom_path	            as source_bom_path,
        stock_site	                as stock_site,
        root_src_unit_price	        as root_src_unit_price
    from source src
)
select
        cast(substring(source_system,1,255) as text(255) )              as source_system  ,
        cast(upper(substring(active_flag,1,255)) as text(255) )         as active_flag  ,
        cast(substring(version_id,1,255) as text(255) )                 as version_id  ,
        cast(eff_date as timestamp_ntz(9) )                             as eff_date  ,
        cast(creation_date_time as timestamp_ntz(9) )                   as creation_date_time  ,
        cast(expir_date as timestamp_ntz(9) )                           as expir_date  ,
        cast(source_updated_datetime as timestamp_ntz(9) )              as source_updated_datetime  ,
        cast(substring(transaction_currency,1,255) as text(255) )       as transaction_currency  ,
        cast(substring(transaction_uom,1,255) as text(255) )            as transaction_uom  ,
        cast(substring(root_company_code,1,255) as text(255) )          as root_company_code  ,
        cast(substring(root_src_item_identifier,1,255) as text(255) )   as root_src_item_identifier  ,
        cast(substring(root_src_variant_code,1,255) as text(255) )      as root_src_variant_code  ,
        cast(root_src_item_guid as text(255) )                          as root_src_item_guid  ,
        cast(substring(comp_src_item_identifier,1,255) as text(255) )   as comp_src_item_identifier  ,
        cast(substring(comp_src_variant_code,1,255) as text(255) )      as comp_src_variant_code  ,
        cast(comp_src_item_guid as text(255) )                          as comp_src_item_guid  ,
        cast(comp_consumption_qty as number(38,10) )                    as comp_consumption_qty  ,
        cast(comp_consumption_unit as number(38,10) )                   as comp_consumption_unit  ,
        cast(comp_cost_price as number(38,10) )                         as comp_cost_price  ,
        cast(comp_cost_price_unit as number(38,10) )                    as comp_cost_price_unit  ,
        cast(comp_item_unit_cost as number(38,10) )                     as comp_item_unit_cost  ,
        cast(comp_bom_level as number(38,0) )                           as comp_bom_level  ,
        cast(substring(comp_calctype_desc,1,255) as text(255) )         as comp_calctype_desc  ,
        cast(substring(comp_cost_group_id,1,255) as text(255) )         as comp_cost_group_id  ,
        cast(substring(parent_item_indicator,1,255) as text(255) )      as parent_item_indicator  ,
        cast(substring(source_business_unit_code,1,255) as text(255) )  as source_business_unit_code  ,
        cast(substring(source_bom_path,1,255) as text(255) )            as source_bom_path  ,
        cast(substring(stock_site,1,255) as text(255) )                 as stock_site  ,
        cast(root_src_unit_price as number(38,10) )                     as root_src_unit_price  ,
         {{
            dbt_utils.surrogate_key(
                [   "source_system",
                    "creation_date_time",
                    "root_src_item_identifier",
                    "root_src_variant_code",
                    "comp_src_item_identifier",
                    "comp_src_variant_code",
                    "source_business_unit_code" ,
                    "source_bom_path" ,
                    "stock_site"
                ]
            )
        }}                                                              as unique_key
    from final

