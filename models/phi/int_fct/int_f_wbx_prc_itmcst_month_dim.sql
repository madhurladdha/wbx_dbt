{{
    config(
        on_schema_change="sync_all_columns",
        tags=["item_cost","prc_item_cost","procurement"],
        )
}}

--read variable for lookback period
{% set var_calc = env_var("DBT_PROCUREMENT_LOOKBACK") %}

with cte_date_src as (select * from {{ ref('src_dim_date')}} ),
cte_dim_date_range as 
(
  select 
        min(fiscal_period_begin_dt) fiscal_period_begin_dt,
        max(fiscal_period_end_dt) fiscal_period_end_dt  
    from cte_date_src 
    where calendar_date =  add_months(current_date,-1) or calendar_date =  add_months(current_date, -{{ var_calc }})
),
curr_dim_date as (
      select 
        distinct 
        fiscal_period_begin_dt,
        fiscal_period_end_dt,
        fiscal_year_period_no 
    from cte_date_src
    where calendar_date >=(select fiscal_period_begin_dt from cte_dim_date_range) 
    and  calendar_date <=  (select fiscal_period_end_dt from cte_dim_date_range)
),
po_receipt as (
    select * from {{ ref('fct_wbx_fin_prc_po_receipt')}} r
    join curr_dim_date d on 
    r.po_received_date between d.fiscal_period_begin_dt and fiscal_period_end_dt
),

item_master as (
    select 
        item_guid,
        source_item_identifier,
        source_business_unit_code,
        primary_uom,
        business_unit_address_guid,
        item_type,
        source_system,
        case_item_number
    from {{ref('dim_wbx_item')}}
),
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
source as (
    select 
        trim(r.source_system)                   as source_system,
        trim(r.source_item_identifier)          as source_item_identifier,
        trim(b.consolidated_shipment_dc_code)   as source_business_unit_code,
        r.item_guid                             as item_guid,
        b.consolidated_shipment_dc_guid         as business_unit_address_guid,
        trim(i.primary_uom)                     as primary_uom,
        trim(r.transaction_uom)                 as transaction_uom,
        r.fiscal_year_period_no                 as fiscal_year_period_no,
        r.receipt_received_quantity             as receipt_received_quantity,
        r.phi_receipt_received_amt              as phi_receipt_received_amt,
        r.base_currency                         as base_currency,
        r.base_receipt_received_amt             as base_receipt_received_amt,
        trim(i.item_type)                       as item_type
  from po_receipt r
       inner join item_master i
            on i.source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
            and r.item_guid = i.item_guid
            and r.business_unit_address_guid = i.business_unit_address_guid
       left outer join plant_dc b
            on r.source_business_unit_code = b.source_business_unit_code
            and r.source_system = b.source_system    
             
 WHERE    
       r.business_unit_address_guid is not null 
       and r.item_guid is not null 
       and i.item_type in ('INGREDIENT', 'PACKAGING')
       or (i.source_item_identifier like 'R%'
       or i.source_item_identifier like 'P%')
),


curr_conv_rt as (
    select
        src.source_system                                    as source_system,
        src.source_item_identifier                           as source_item_identifier,
        src.source_business_unit_code                        as source_business_unit_code,
        src.item_guid                                        as item_guid,
        src.business_unit_address_guid                       as business_unit_address_guid,
        case when src.item_type='INGREDIENT'
        then 'LB' else src.primary_uom end                   as primary_uom,
        src.transaction_uom                                  as transaction_uom,
        src.fiscal_year_period_no                            as fiscal_year_period_no,
        src.receipt_received_quantity                        as receipt_received_quantity,
        src.phi_receipt_received_amt                         as phi_receipt_received_amt,
        src.base_currency                                    as base_currency,
        src.base_receipt_received_amt                        as base_receipt_received_amt,
       case when src.ITEM_TYPE<> 'INGREDIENT' then 1
       when src.PRIMARY_UOM = 'LB' then {{ent_dbt_package.lkp_constants("DEFAULT_CONVERSION_RATE")}}    
       when src.PRIMARY_UOM = 'KG' then {{ent_dbt_package.lkp_constants("KG_LB_CONVERSION_RATE")}}
       else (CONVERSION_RATE * {{ent_dbt_package.lkp_constants("KG_LB_CONVERSION_RATE")}}) end as conversion_rate
    from source src
    left outer join
    {{
        ent_dbt_package.lkp_uom("src.item_guid","src.primary_uom","src.transaction_uom","conversion_rate",
        )
    }}
),
agg_po as (
    select
        source_system                                       as source_system,
        source_item_identifier                              as source_item_identifier,
        source_business_unit_code                           as source_business_unit_code,
        item_guid                                           as item_guid,
        business_unit_address_guid                          as business_unit_address_guid,
        primary_uom                                         as primary_uom,
        transaction_uom                                     as transaction_uom,
        fiscal_year_period_no                               as fiscal_year_period_no,
        sum(conversion_rate * receipt_received_quantity)    as receipt_received_quantity,
        sum(phi_receipt_received_amt)                       as receipt_received_amt,
        base_currency                                       as base_currency,
        sum(base_receipt_received_amt)                      as base_receipt_received_amt
        
    from curr_conv_rt
    group by
    source_system,source_item_identifier,source_business_unit_code,
    item_guid,business_unit_address_guid,primary_uom,transaction_uom,
    fiscal_year_period_no,base_currency 
),
final as (
    select 
        source_system                                    as source_system,
        source_item_identifier                           as source_item_identifier,
        item_guid                                        as item_guid,
        source_business_unit_code                        as source_business_unit_code,
        business_unit_address_guid                       as business_unit_address_guid,
        fiscal_year_period_no                            as fiscal_period_number,
        primary_uom                                      as input_uom,
        '02'                                             as cost_method,
        case when receipt_received_amt=0 or 
        receipt_received_quantity=0 then 0 
        else (receipt_received_amt/receipt_received_quantity) end as phi_unit_cost,
        'USD'                                            as phi_currency_code,
        'N'                                              as cost_rollfwd_flag,
        '1'                                              as source_exchange_rate,
        (base_receipt_received_amt/receipt_received_quantity) as base_unit_cost,
        base_currency                                    as base_currency_code,
        case when base_unit_cost = 0 then 0
        else (phi_unit_cost/ base_unit_cost) end         as phi_conv_rt,
        current_timestamp()                              as load_date,
        current_timestamp()                              as update_date

    from agg_po
)
select
    cast(substring(source_system,1,255) as text(255) )           as source_system  ,
    cast(substring(source_item_identifier,1,60) as text(60) )    as source_item_identifier  ,
    cast(item_guid as text(255) )                                as item_guid  ,
    cast(substring(source_business_unit_code,1,24) as text(24) ) as source_business_unit_code  ,
    cast(business_unit_address_guid as text(255) )               as business_unit_address_guid  ,
    cast(fiscal_period_number as number(38,0) )                  as fiscal_period_number  ,
    cast(substring(input_uom,1,6) as text(6) )                   as input_uom  ,
    cast(substring(cost_method,1,6) as text(6) )                 as cost_method  ,
    cast(phi_unit_cost as number(14,4) )                         as phi_unit_cost  ,
    cast(substring(phi_currency_code,1,6) as text(6) )           as phi_currency_code  ,
    cast(substring(cost_rollfwd_flag,1,1) as text(1) )           as cost_rollfwd_flag  ,
    cast(source_exchange_rate as number(14,7) )                  as source_exchange_rate  ,
    cast(base_unit_cost as number(14,4) )                        as base_unit_cost  ,
    cast(substring(base_currency_code,1,6) as text(6) )          as base_currency_code  ,
    cast(phi_conv_rt as number(14,7) )                           as phi_conv_rt,
    
    {{ dbt_utils.surrogate_key([
            "source_system",
            "source_item_identifier",
            "source_business_unit_code",
            "fiscal_period_number",
            "cost_method"
        ]) }}                                                    as unique_key
from final