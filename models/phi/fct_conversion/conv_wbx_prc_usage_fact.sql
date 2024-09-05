   
   {{
    config(
        materialized = env_var('DBT_MAT_VIEW'),
        tags=["procurement", "usage","prc_usage"]
    )
}}

WITH old_fct AS 
(
    SELECT * FROM {{source('FACTS_FOR_COMPARE','prc_wtx_usage_fact')}} WHERE SOURCE_SYSTEM = '{{env_var("DBT_SOURCE_SYSTEM")}}' and  {{env_var("DBT_PICK_FROM_CONV")}}='Y'
),

converted_fct as (

    select
        source_system,
        source_item_identifier,
        item_guid,
        source_business_unit_code,
        business_unit_address_guid,
        fiscal_period_number,
      --  gl_cat_c,
        input_uom,
        ending_inventory_qty,
        beginning_inventory_qty,
        receipt_qty,
        transfer_in_qty,
        transfer_out_qty,
        load_date,
        update_date,
        transfer_in_intercompany_qty,
        transfer_out_intercompany_qty,
        transfer_in_intracompany_qty,
        transfer_out_intracompany_qty

    from old_fct

)

select 
    cast(substring(source_system,1,255) as text(255) )              as source_system,
    cast(substring(source_item_identifier,1,255) as text(255) )     as source_item_identifier,
    cast({{ dbt_utils.surrogate_key(
    ["source_system","source_item_identifier"]) }} as text(255) )   as item_guid  ,
    cast(substring(source_business_unit_code,1,24) as text(24) )    as source_business_unit_code  ,
    cast({{ dbt_utils.surrogate_key(["source_system",
    "source_business_unit_code","'PLANT_DC'"]) }} as text(255) )    as business_unit_address_guid  ,
    cast(fiscal_period_number as number(38,0) )                     as fiscal_period_number,
    cast(substring(input_uom,1,6) as text(6) )                      as input_uom,
   -- cast(substring(gl_cat_c,1,6) as text(6) )                       as gl_cat_c, 
    cast(ending_inventory_qty as number(38,10) )                    as ending_inventory_qty,
    cast(beginning_inventory_qty as number(38,10) )                 as beginning_inventory_qty,
    cast(receipt_qty as number(38,10) )                             as receipt_qty,
    cast(transfer_in_qty as number(38,10) )                         as transfer_in_qty,
    cast(transfer_out_qty as number(38,10) )                        as transfer_out_qty,
    cast(transfer_in_intercompany_qty as number(38,10) )            as transfer_in_intercompany_qty,
    cast(transfer_out_intercompany_qty as number(38,10) )           as transfer_out_intercompany_qty,
    cast(transfer_in_intracompany_qty as number(38,10) )            as transfer_in_intracompany_qty,
    cast(transfer_out_intracompany_qty as number(38,10) )           as transfer_out_intracompany_qty,
    cast(current_timestamp() as timestamp_ntz(9) )                  as load_date,
    cast(current_timestamp() as timestamp_ntz(9) )                  as update_date,
    {{ dbt_utils.surrogate_key([
            "cast(substring(source_system,1,255) as text(255) ) ",
            "cast(substring(source_item_identifier,1,255) as text(255) )",
            "cast(substring(source_business_unit_code,1,255) as text(255) )",
            "cast(fiscal_period_number as number(38,0) ) "
        ]) }}                                                       as unique_key

from converted_fct
