{{
    config(
        on_schema_change="sync_all_columns",
        tags=["procurement", "usage","prc_usage"],
        snowflake_warehouse= env_var("DBT_WBX_SF_WH")
        )
}}

{% set var_calc = env_var("DBT_PROCUREMENT_LOOKBACK") %}

with curr_dim_date as (
    select distinct FISCAL_YEAR_PERIOD_NO from {{ ref('src_dim_date')}} 
    where calendar_date between add_months(current_date, -{{ var_calc }}) and add_months(current_date,-1)  
),

juliana_date as (
    select * from {{ ref('src_dim_date')}} 
),

inv_mnthly_ldgr as (
    select * from {{ ref('fct_wbx_inv_mnthly_ldgr')}} r
    join curr_dim_date d on 
    r.fiscal_period_number = d.fiscal_year_period_no

),
/* Getting the item level distinct attributes.  So removed references to BU */
item_master as (
    select 
        distinct item_guid,
        source_item_identifier,
        --source_business_unit_code,
        primary_uom,
        --business_unit_address_guid,
        item_type,
        source_system
        --case_item_number
    from {{ref('dim_wbx_item')}}
),
/* Do not need or use this CTE */
/*
item_master1 as (
    select 
        item_guid, 
        business_unit_address_guid, 
        source_system
    from {{ref('dim_wbx_item')}} 
),
*/

plant_dc as (
    select distinct
        a.source_system,
        a.source_business_unit_code,
        b.source_business_unit_code consolidated_shipment_dc_code,
        a.plantdc_address_guid,
        b.plantdc_address_guid consolidated_shipment_dc_guid
    from {{ ref('dim_wbx_plant_dc')}} a, 
         {{ ref('dim_wbx_plant_dc')}} b
    where a.consolidated_shipment_dc_name =b.source_business_unit_code
               and a.source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
               and b.source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
),

/* Do not need or use this CTE */
/*
lkp_item_master as (
    select * from {{ref('dim_wbx_item')}}
),
*/

source as (
    select 
        f.source_system                             as source_system,
        f.source_item_identifier                    as source_item_identifier,
        trim(b.consolidated_shipment_dc_code)       as source_business_unit_code,
        f.fiscal_period_number                      as fiscal_period_number,
        i1.item_type                                as item_type,
        i1.primary_uom                              as primary_uom,
        sum(f.ending_inventory_qty)                 as ending_inventory_qty,
        sum(f.beginning_inventory_qty)              as beginning_inventory_qty,
        sum(f.po_receipt_qty)                       as po_receipt_qty,
        f.item_guid                                 as item_guid,
        b.consolidated_shipment_dc_guid             as business_unit_address_guid,
        sum(transfer_in_intercompany_qty)           as transfer_in_intercompany_qty, 
        sum(transfer_out_intercompany_qty)          as transfer_out_intercompany_qty, 
        sum(transfer_in_intracompany_qty)           as transfer_in_intracompany_qty, 
        sum(transfer_out_intracompany_qty)          as transfer_out_intracompany_qty
    from inv_mnthly_ldgr f
    /* Changing this to a join only on distinct item for item level attributes.*/
    inner join item_master i1 
        on  i1.source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
        and f.item_guid = i1.item_guid
    /* Changing this to an inner join.  Have to have this for a proper BU */
    inner join plant_dc b 
        on f.source_business_unit_code = b.source_business_unit_code
        and f.source_system = b.source_system

/* Original code was missing the additional or for Items that start w/ R or P */
    where (i1.item_type in ('INGREDIENT', 'PACKAGING') or i1.source_item_identifier like 'R%' or i1.source_item_identifier like 'P%')
        and ( beginning_inventory_qty <> 0 or ending_inventory_qty <> 0 
        or po_receipt_qty <> 0 or transfer_in_intercompany_qty <> 0 
        or transfer_out_intercompany_qty <> 0 or transfer_in_intracompany_qty <> 0 
        or transfer_out_intracompany_qty <> 0) 
    group by f.source_system ,f.source_item_identifier,f.item_guid,
    trim(b.consolidated_shipment_dc_code),b.consolidated_shipment_dc_guid,
    f.fiscal_period_number,item_type,primary_uom
),
curr_conv_rt as (
    select
        src.source_system                          as source_system,
        src.source_item_identifier                 as source_item_identifier,
        src.item_guid                              as item_guid,
        src.source_business_unit_code              as source_business_unit_code,
        src.business_unit_address_guid             as business_unit_address_guid,
        src.fiscal_period_number                   as fiscal_period_number,
        src.item_type                              as item_type,
        src.primary_uom                            as primary_uom,
       -- case when it.gl_class is null then '-' 
      --  else i1.gl_class end                        as gl_cat_c,
      
        case when src.primary_uom='LB' then {{ent_dbt_package.lkp_constants("DEFAULT_CONVERSION_RATE")}} 
        when src.primary_uom='KG' then {{ent_dbt_package.lkp_constants("KG_LB_CONVERSION_RATE")}}
        when src.primary_uom='CW' then {{ent_dbt_package.lkp_constants("CW_LB_CONVERSION_RATE")}}
        when v_lb_conv.conversion_rate is not null then v_lb_conv.conversion_rate
        when v_kg_conv.conversion_rate is not null then v_kg_conv.conversion_rate *{{ent_dbt_package.lkp_constants("KG_LB_CONVERSION_RATE")}}
        when v_cwt_conv.conversion_rate is not null then v_cwt_conv.conversion_rate * {{ent_dbt_package.lkp_constants("CW_LB_CONVERSION_RATE")}}
        else 1 end                                                          as var_pri_to_lb_conv,
        case when var_pri_to_lb_conv is null then 0
        else var_pri_to_lb_conv end                as v_pri_to_lb_conv,
        src.ending_inventory_qty                   as ending_inventory_qty,
        src.beginning_inventory_qty                as beginning_inventory_qty,
        src.po_receipt_qty                         as po_receipt_qty,       
        src.transfer_in_intercompany_qty           as transfer_in_intercompany_qty, 
        src.transfer_out_intercompany_qty          as transfer_out_intercompany_qty, 
        src.transfer_in_intracompany_qty           as transfer_in_intracompany_qty, 
        src.transfer_out_intracompany_qty          as transfer_out_intracompany_qty
    from source src
    left outer join juliana_date dt on dt.fiscal_year_period_no = src.fiscal_period_number
    left outer join 
    {{
        ent_dbt_package.lkp_uom("src.item_guid","src.primary_uom","'LB'","v_lb_conv",
        )
    }}
    left outer join
    {{
        ent_dbt_package.lkp_uom("src.item_guid","src.primary_uom","'KG'","v_kg_conv",
        )
    }}
    left outer join
    {{
        ent_dbt_package.lkp_uom("src.item_guid","src.primary_uom","'CW'","v_cwt_conv",
        )
    }}
),
final as (
    select
        rt.source_system                                    as source_system,
        rt.source_item_identifier                           as source_item_identifier,
        rt.item_guid                                        as item_guid,
        rt.source_business_unit_code                        as source_business_unit_code,
        rt.business_unit_address_guid                       as business_unit_address_guid,
        rt.fiscal_period_number                             as fiscal_period_number,
        case when rt.item_type= 'INGREDIENT' then 'LB'
        else rt.primary_uom end                             as input_uom,                         
        rt.primary_uom                                      as primary_uom,
     --   rt.gl_cat_c                                         as gl_cat_c,
        rt.var_pri_to_lb_conv                               as var_pri_to_lb_conv,
        case when rt.item_type= 'INGREDIENT' 
        then rt.ending_inventory_qty* v_pri_to_lb_conv
        else rt.ending_inventory_qty end                    as v_ending_inventory_qty,
        case when v_ending_inventory_qty is null 
        then 0 else v_ending_inventory_qty end              as ending_inventory_qty,
        case when rt.item_type= 'INGREDIENT' 
        then rt.beginning_inventory_qty* v_pri_to_lb_conv
        else rt.beginning_inventory_qty end                 as v_beginning_inventory_qty,
        case when v_beginning_inventory_qty is null
        then 0 else v_beginning_inventory_qty end           as beginning_inventory_qty,
        case when rt.item_type= 'INGREDIENT'
        then rt.po_receipt_qty* v_pri_to_lb_conv
        else po_receipt_qty end                             as v_receipt_qty,
        case when v_receipt_qty is null then 0
        else v_receipt_qty end                              as receipt_qty,
        null                                                as transfer_in_qty,
        null                                                as transfer_out_qty,       
        case when rt.item_type= 'INGREDIENT' then 
        rt.transfer_in_intercompany_qty* v_pri_to_lb_conv
        else rt.transfer_in_intercompany_qty end            as v_transfer_in_intercompany_qty,
        case when v_transfer_in_intercompany_qty is null 
        then 0 else v_transfer_in_intercompany_qty end      as transfer_in_intercompany_qty,
        case when rt.item_type= 'INGREDIENT' then
        rt.transfer_out_intercompany_qty* v_pri_to_lb_conv
        else transfer_out_intercompany_qty end              as v_transfer_out_intercompany_qty,
        case when v_transfer_out_intercompany_qty is null
        then 0 else v_transfer_out_intercompany_qty end     as transfer_out_intercompany_qty,
        case when rt.item_type= 'INGREDIENT' then 
        rt.transfer_in_intracompany_qty* v_pri_to_lb_conv
        else rt.transfer_in_intracompany_qty end            as v_transfer_in_intracompany_qty,
        case when v_transfer_in_intracompany_qty is null
        then 0 else v_transfer_in_intracompany_qty end      as transfer_in_intracompany_qty,
        case when rt.item_type= 'INGREDIENT' then  
        rt.transfer_out_intracompany_qty* v_pri_to_lb_conv
        else rt.transfer_out_intracompany_qty end           as v_transfer_out_intracompany_qty,
        case when v_transfer_out_intracompany_qty is null
        then 0 else v_transfer_out_intracompany_qty end     as transfer_out_intracompany_qty 
      
    from curr_conv_rt rt
)
select 
    cast(substring(source_system,1,255) as text(255) )              as source_system,
    cast(substring(source_item_identifier,1,255) as text(255) )     as source_item_identifier,
    cast(item_guid as text(255) )                                   as item_guid,
    cast(substring(source_business_unit_code,1,255) as text(255) )  as source_business_unit_code,
    cast(business_unit_address_guid as text(255) )                  as business_unit_address_guid,
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
    
   
from final
