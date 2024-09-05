{{
    config(
        tags = ["sls","sales","forecast","sls_forecast"]
    )
}}

with source as (
    select * from {{ ref('stg_f_wtx_sls_forecast')}}
),
item_master as (select distinct source_system,source_item_identifier from {{ ref('dim_wbx_item') }}),
snapshot_date as (
   select snapshot_date from (select *,rank() over (order by snapshot_date desc) as rank 
   from {{ ref('stg_d_wtx_lkp_snapshot_date')}} ) where rank=1
),
planning_date as (
    select * from {{ ref('dim_wbx_planning_date_oc')}}
),
planning_customer as (
    select distinct  
        trim(trade_type_code) as trade_type_code ,
        trade_type_desc,
        trade_type_seq,
        market_code,
        market_desc,
        market_code_seq,
        sub_market_code,
        sub_market_desc,
        sub_market_code_seq,
        trade_class_code,
        trade_class_desc,
        trade_class_seq,
        trade_group_code,
        trade_group_desc,
        trade_group_seq,
        trade_sector_code,
        trade_sector_desc,
        trade_sector_seq
    from {{ref('stg_d_wbx_customer_ext')}} 
    where (trim(trade_type_code) is not null and  trim(trade_type_code)!='')
    order by trade_type_code
),

/*  29-May-2023: replaced the ref for this from src_sls_scenario_dim to dim_wbx_scenario.
*/
sls_scenario_dim as (
    select 
    source_system
    ,scenario_guid
    ,scenario_id
    ,scenario_code
    ,scenario_desc 
    from {{ ref('dim_wbx_scenario')}} 
    where source_system='{{env_var("DBT_SOURCE_SYSTEM")}}'
),
customer_planning as (
    select trade_type_code from {{ ref('stg_d_wbx_customer_planning')}}
 ),
stage as (
    select 
         src.source_system                                          as source_system
        ,source_item_identifier                                     as source_item_identifier
        ,{{dbt_utils.surrogate_key(["src.source_system",
        "ltrim(rtrim(src.source_item_identifier))",])}}             as item_guid
        ,lpad(source_item_identifier,5,0)                           as lkp_source_item_identifier
        ,plan_source_customer_code                                  as plan_source_customer_code
        ,src.calendar_date                                          as calendar_date
        ,frozen_forecast                                            as frozen_forecast
        ,decode(upper(isonpromo_si),'FALSE','N','TRUE','Y')         as isonpromo_si
        ,decode(upper(isonpromo_so),'FALSE','N','TRUE','Y')         as isonpromo_so
        ,decode(upper(ispreorpostpromo_si),'FALSE','N','TRUE','Y')  as ispreorpostpromo_si
        ,decode(upper(ispreorpostpromo_so),'FALSE','N','TRUE','Y')  as ispreorpostpromo_so
        ,decode(upper(listingactive),'FALSE','N','TRUE','Y')        as listingactive
        ,decode(upper(is_vol_total_nonzero),'FALSE','N','TRUE','Y') as is_vol_total_nonzero
        ,total_baseretentionpercentage                              as total_baseretentionpercentage
        ,total_si_preorpostdippercentage                            as total_si_preorpostdippercentage
        ,total_so_preorpostdippercentage                            as total_so_preorpostdippercentage
        ,qty_ca_stat_base_fc_si                                     as qty_ca_stat_base_fc_si
        ,qty_ca_stat_base_fc_so                                     as qty_ca_stat_base_fc_so
        ,qty_ca_override_si                                         as qty_ca_override_si
        ,qty_ca_override_so                                         as qty_ca_override_so
        ,qty_ca_effective_base_fc_si                                as qty_ca_effective_base_fc_si
        ,qty_ca_effective_base_fc_so                                as qty_ca_effective_base_fc_so
        ,qty_ca_promo_total_si                                      as qty_ca_promo_total_si
        ,qty_ca_promo_total_so                                      as qty_ca_promo_total_so
        ,qty_ca_cannib_loss_si                                      as qty_ca_cannib_loss_si
        ,qty_ca_cannib_loss_so                                      as qty_ca_cannib_loss_so
        ,qty_ca_pp_dip_si                                           as qty_ca_pp_dip_si
        ,qty_ca_pp_dip_so                                           as qty_ca_pp_dip_so
        ,qty_ca_total_si                                            as qty_ca_total_si
        ,qty_ca_total_so                                            as qty_ca_total_so
        ,qty_ca_si_actual                                           as qty_ca_si_actual
        ,qty_ca_so_actual                                           as qty_ca_so_actual
        ,qty_ca_total_adjust_si                                     as qty_ca_total_adjust_si
        ,qty_ca_total_adjust_so                                     as qty_ca_total_adjust_so
        ,cust_idx                                                   as cust_idx
        ,prod_idx                                                   as prod_idx
        ,scen_idx                                                   as scen_idx
        ,scen_name                                                  as scen_name
        ,scen_code                                                  as scen_code
        ,trade_type_code                                            as trade_type_code
        ,trade_type_desc                                            as trade_type_desc
        ,trade_type_seq                                             as trade_type_seq
        ,market_code                                                as market_code
        ,market_desc                                                as market_desc
        ,market_code_seq                                            as market_code_seq
        ,sub_market_code                                            as sub_market_code
        ,sub_market_desc                                            as sub_market_desc 
        ,sub_market_code_seq                                        as sub_market_code_seq
        ,trade_class_code                                           as trade_class_code
        ,trade_class_desc                                           as trade_class_desc
        ,trade_class_seq                                            as trade_class_seq
        ,trade_group_code                                           as trade_group_code
        ,trade_group_desc                                           as trade_group_desc
        ,trade_group_seq                                            as trade_group_seq
        ,trade_sector_code                                          as trade_sector_code
        ,trade_sector_desc                                          as trade_sector_desc
        ,trade_sector_seq                                           as trade_sector_seq
        ,snapshot_date                                              as snapshot_date
    from source src
        left join planning_date dt
        on src.source_system=dt.source_system and
        src.calendar_date =dt.calendar_date 
        left join snapshot_date snap on 1=1
        left join planning_customer plan_cust
        on trim(src.plan_source_customer_code) = trim(plan_cust.trade_type_code)
    where 
        ( nvl(total_baseretentionpercentage,0) <> 0  or  
        nvl(total_si_preorpostdippercentage,0) <> 0 or 
        nvl(total_so_preorpostdippercentage,0) <> 0 or 
        --( nvl(is_vol_total_nonzero,0) <> 0 or 
        nvl(qty_ca_stat_base_fc_si,0) <> 0  or
        nvl(qty_ca_stat_base_fc_so,0) <> 0  or 
        nvl(qty_ca_override_si,0) <> 0  or
        nvl(qty_ca_override_so,0) <> 0  or 
        nvl(qty_ca_effective_base_fc_si,0) <> 0  or
        nvl(qty_ca_effective_base_fc_so,0) <> 0  or 
        nvl(qty_ca_promo_total_si,0) <> 0  or
        nvl(qty_ca_promo_total_so,0) <> 0  or 
        nvl(qty_ca_cannib_loss_si,0) <> 0  or 
        nvl(qty_ca_cannib_loss_so,0) <> 0  or 
        nvl(qty_ca_pp_dip_si,0) <> 0  or
        nvl(qty_ca_pp_dip_so,0) <> 0  or
        nvl(qty_ca_total_si,0) <> 0  or
        nvl(qty_ca_total_so,0) <> 0  or
        nvl(qty_ca_si_actual,0) <> 0  or 
        nvl(qty_ca_so_actual,0) <> 0  or
        nvl(qty_ca_total_adjust_si,0) <> 0  or 
        nvl(qty_ca_total_adjust_so,0) <> 0 )
),
tfm as (
    select
        stg.source_system                                   as source_system,
        stg.source_item_identifier                          as source_item_identifier,
        stg.item_guid                                       as item_guid,
        plan_source_customer_code                           as plan_source_customer_code,
        0                                                   as plan_customer_addr_number_guid,
        calendar_date                                       as calendar_date,
        frozen_forecast                                     as frozen_forecast,
        isonpromo_si                                        as isonpromo_si,
        isonpromo_so                                        as isonpromo_so,
        ispreorpostpromo_si                                 as ispreorpostpromo_si,
        ispreorpostpromo_so                                 as ispreorpostpromo_so,
        listingactive                                       as listingactive,
        total_baseretentionpercentage                       as total_baseretentionpercentage,
        total_si_preorpostdippercentage                     as total_si_preorpostdippercentage,
        total_so_preorpostdippercentage                     as total_so_preorpostdippercentage,
        is_vol_total_nonzero                                as is_vol_total_nonzero,
        qty_ca_stat_base_fc_si                              as qty_ca_stat_base_fc_si,
        qty_ca_stat_base_fc_so                              as qty_ca_stat_base_fc_so,
        qty_ca_override_si                                  as qty_ca_override_si,
        qty_ca_override_so                                  as qty_ca_override_so,
        qty_ca_effective_base_fc_si                         as qty_ca_effective_base_fc_si,
        qty_ca_effective_base_fc_so                         as qty_ca_effective_base_fc_so,
        qty_ca_promo_total_si                               as qty_ca_promo_total_si,
        qty_ca_promo_total_so                               as qty_ca_promo_total_so,
        qty_ca_cannib_loss_si                               as qty_ca_cannib_loss_si,
        qty_ca_cannib_loss_so                               as qty_ca_cannib_loss_so,
        qty_ca_pp_dip_si                                    as qty_ca_pp_dip_si,
        qty_ca_pp_dip_so                                    as qty_ca_pp_dip_so,
        qty_ca_total_si                                     as qty_ca_total_si,
        qty_ca_total_so                                     as qty_ca_total_so,
        qty_ca_si_actual                                    as qty_ca_si_actual,
        qty_ca_so_actual                                    as qty_ca_so_actual,
        qty_ca_total_adjust_si                              as qty_ca_total_adjust_si,
        qty_ca_total_adjust_so                              as qty_ca_total_adjust_so,
        uom_ca_kg_lkp.conversion_rate                       as v_ca_kg_conv,
        qty_ca_stat_base_fc_si*v_ca_kg_conv                 as qty_kg_stat_base_fc_si,
        qty_ca_stat_base_fc_so*v_ca_kg_conv                 as qty_kg_stat_base_fc_so,
        qty_ca_override_si*v_ca_kg_conv                     as qty_kg_override_si,
        qty_ca_override_so*v_ca_kg_conv                     as qty_kg_override_so,
        qty_ca_effective_base_fc_si*v_ca_kg_conv            as qty_kg_effective_base_fc_si,
        qty_ca_effective_base_fc_so*v_ca_kg_conv            as qty_kg_effective_base_fc_so,
        qty_ca_promo_total_si*v_ca_kg_conv                  as qty_kg_promo_total_si,
        qty_ca_promo_total_so*v_ca_kg_conv                  as qty_kg_promo_total_so,
        qty_ca_cannib_loss_si*v_ca_kg_conv                  as qty_kg_cannib_loss_si,
        qty_ca_cannib_loss_so*v_ca_kg_conv                  as qty_kg_cannib_loss_so,
        qty_ca_pp_dip_si*v_ca_kg_conv                       as qty_kg_pp_dip_si,
        qty_ca_pp_dip_so*v_ca_kg_conv                       as qty_kg_pp_dip_so,
        qty_ca_total_si*v_ca_kg_conv                        as qty_kg_total_si,
        qty_ca_total_so*v_ca_kg_conv                        as qty_kg_total_so,
        qty_ca_si_actual*v_ca_kg_conv                       as qty_kg_si_actual,
        qty_ca_so_actual*v_ca_kg_conv                       as qty_kg_so_actual,
        qty_ca_total_adjust_si*v_ca_kg_conv                 as qty_kg_total_adjust_si,
        qty_ca_total_adjust_so*v_ca_kg_conv                 as qty_kg_total_adjust_so,
        uom_ca_pl_lkp.conversion_rate                       as v_ca_pl_conv,
        qty_ca_stat_base_fc_si*v_ca_pl_conv                 as qty_ul_stat_base_fc_si,
        qty_ca_stat_base_fc_so*v_ca_pl_conv                 as qty_ul_stat_base_fc_so,
        qty_ca_override_si*v_ca_pl_conv                     as qty_ul_override_si,
        qty_ca_override_so*v_ca_pl_conv                     as qty_ul_override_so,
        qty_ca_effective_base_fc_si*v_ca_pl_conv            as qty_ul_effective_base_fc_si,
        qty_ca_effective_base_fc_so*v_ca_pl_conv            as qty_ul_effective_base_fc_so,
        qty_ca_promo_total_si*v_ca_pl_conv                  as qty_ul_promo_total_si,
        qty_ca_promo_total_so*v_ca_pl_conv                  as qty_ul_promo_total_so,
        qty_ca_cannib_loss_si*v_ca_pl_conv                  as qty_ul_cannib_loss_si,
        qty_ca_cannib_loss_so*v_ca_pl_conv                  as qty_ul_cannib_loss_so,
        qty_ca_pp_dip_si*v_ca_pl_conv                       as qty_ul_pp_dip_si,
        qty_ca_pp_dip_so*v_ca_pl_conv                       as qty_ul_pp_dip_so,
        qty_ca_total_si*v_ca_pl_conv                        as qty_ul_total_si,
        qty_ca_total_so*v_ca_pl_conv                        as qty_ul_total_so,
        qty_ca_si_actual*v_ca_pl_conv                       as qty_ul_si_actual,
        qty_ca_so_actual*v_ca_pl_conv                       as qty_ul_so_actual,
        qty_ca_total_adjust_si*v_ca_pl_conv                 as qty_ul_total_adjust_si,
        qty_ca_total_adjust_so*v_ca_pl_conv                 as qty_ul_total_adjust_so,
        cust_idx                                            as cust_idx,
        prod_idx                                            as prod_idx,
        scen_idx                                            as scen_idx,
        scen_lkp.scenario_code                              as scen_code,
        scen_lkp.scenario_desc                              as scen_name,
        scen_lkp.scenario_guid                              as scenario_guid,
        snapshot_date                                       as snapshot_date
    from stage stg
    inner join sls_scenario_dim scen_lkp
    on stg.source_system=scen_lkp.source_system
    and stg.scen_idx=scen_lkp.scenario_id
    left join customer_planning cust_plan_lkp
    on stg.plan_source_customer_code=cust_plan_lkp.trade_type_code
    left join
    {{
        ent_dbt_package.lkp_uom("stg.item_guid","'CA'","'KG'","uom_ca_kg_lkp",)
	}}
    left join
    {{
        ent_dbt_package.lkp_uom("stg.item_guid","'CA'","'PL'","uom_ca_pl_lkp",)
	}}

),
final as (
    select 
        cast(substring(source_system,1,255) as text(255) )                  as source_system  ,
        cast(substring(source_item_identifier,1,255) as text(255) )         as source_item_identifier  ,
        cast(item_guid as text(255) )                                       as item_guid  ,
        cast(substring(plan_source_customer_code,1,255) as text(255) )      as plan_source_customer_code  ,
        cast(plan_customer_addr_number_guid as text(255) )                  as plan_customer_addr_number_guid  ,
        cast(calendar_date as timestamp_ntz(9) )                            as calendar_date  ,
        cast(substring(frozen_forecast,1,255) as text(255) )                as frozen_forecast  ,
        cast(substring(isonpromo_si,1,20) as text(20) )                     as isonpromo_si  ,
        cast(substring(isonpromo_so,1,20) as text(20) )                     as isonpromo_so  ,
        cast(substring(ispreorpostpromo_si,1,20) as text(20) )              as ispreorpostpromo_si  ,
        cast(substring(ispreorpostpromo_so,1,20) as text(20) )              as ispreorpostpromo_so  ,
        cast(substring(listingactive,1,20) as text(20) )                    as listingactive  ,
        cast(total_baseretentionpercentage as number(38,10) )               as total_baseretentionpercentage  ,
        cast(total_si_preorpostdippercentage as number(38,10) )             as total_si_preorpostdippercentage  ,
        cast(total_so_preorpostdippercentage as number(38,10) )             as total_so_preorpostdippercentage  ,
        cast(substring(is_vol_total_nonzero,1,20) as text(20) )             as is_vol_total_nonzero  ,
        cast(qty_ca_stat_base_fc_si as number(38,10) )                      as qty_ca_stat_base_fc_si  ,
        cast(qty_ca_stat_base_fc_so as number(38,10) )                      as qty_ca_stat_base_fc_so  ,
        cast(qty_ca_override_si as number(38,10) )                          as qty_ca_override_si  ,
        cast(qty_ca_override_so as number(38,10) )                          as qty_ca_override_so  ,
        cast(qty_ca_effective_base_fc_si as number(38,10) )                 as qty_ca_effective_base_fc_si  ,
        cast(qty_ca_effective_base_fc_so as number(38,10) )                 as qty_ca_effective_base_fc_so  ,
        cast(qty_ca_promo_total_si as number(38,10) )                       as qty_ca_promo_total_si  ,
        cast(qty_ca_promo_total_so as number(38,10) )                       as qty_ca_promo_total_so  ,
        cast(qty_ca_cannib_loss_si as number(38,10) )                       as qty_ca_cannib_loss_si  ,
        cast(qty_ca_cannib_loss_so as number(38,10) )                       as qty_ca_cannib_loss_so  ,
        cast(qty_ca_pp_dip_si as number(38,10) )                            as qty_ca_pp_dip_si  ,
        cast(qty_ca_pp_dip_so as number(38,10) )                            as qty_ca_pp_dip_so  ,
        cast(qty_ca_total_si as number(38,10) )                             as qty_ca_total_si  ,
        cast(qty_ca_total_so as number(38,10) )                             as qty_ca_total_so  ,
        cast(qty_ca_si_actual as number(38,10) )                            as qty_ca_si_actual  ,
        cast(qty_ca_so_actual as number(38,10) )                            as qty_ca_so_actual  ,
        cast(qty_ca_total_adjust_si as number(38,10) )                      as qty_ca_total_adjust_si  ,
        cast(qty_ca_total_adjust_so as number(38,10) )                      as qty_ca_total_adjust_so  ,
        cast(qty_kg_stat_base_fc_si as number(38,10) )                      as qty_kg_stat_base_fc_si  ,
        cast(qty_kg_stat_base_fc_so as number(38,10) )                      as qty_kg_stat_base_fc_so  ,
        cast(qty_kg_override_si as number(38,10) )                          as qty_kg_override_si  ,
        cast(qty_kg_override_so as number(38,10) )                          as qty_kg_override_so  ,
        cast(qty_kg_effective_base_fc_si as number(38,10) )                 as qty_kg_effective_base_fc_si  ,
        cast(qty_kg_effective_base_fc_so as number(38,10) )                 as qty_kg_effective_base_fc_so  ,
        cast(qty_kg_promo_total_si as number(38,10) )                       as qty_kg_promo_total_si  ,
        cast(qty_kg_promo_total_so as number(38,10) )                       as qty_kg_promo_total_so  ,
        cast(qty_kg_cannib_loss_si as number(38,10) )                       as qty_kg_cannib_loss_si  ,
        cast(qty_kg_cannib_loss_so as number(38,10) )                       as qty_kg_cannib_loss_so  ,
        cast(qty_kg_pp_dip_si as number(38,10) )                            as qty_kg_pp_dip_si  ,
        cast(qty_kg_pp_dip_so as number(38,10) )                            as qty_kg_pp_dip_so  ,
        cast(qty_kg_total_si as number(38,10) )                             as qty_kg_total_si  ,
        cast(qty_kg_total_so as number(38,10) )                             as qty_kg_total_so  ,
        cast(qty_kg_si_actual as number(38,10) )                            as qty_kg_si_actual  ,
        cast(qty_kg_so_actual as number(38,10) )                            as qty_kg_so_actual  ,
        cast(qty_kg_total_adjust_si as number(38,10) )                      as qty_kg_total_adjust_si  ,
        cast(qty_kg_total_adjust_so as number(38,10) )                      as qty_kg_total_adjust_so  ,
        cast(qty_ul_stat_base_fc_si as number(38,10) )                      as qty_ul_stat_base_fc_si  ,
        cast(qty_ul_stat_base_fc_so as number(38,10) )                      as qty_ul_stat_base_fc_so  ,
        cast(qty_ul_override_si as number(38,10) )                          as qty_ul_override_si  ,
        cast(qty_ul_override_so as number(38,10) )                          as qty_ul_override_so  ,
        cast(qty_ul_effective_base_fc_si as number(38,10) )                 as qty_ul_effective_base_fc_si  ,
        cast(qty_ul_effective_base_fc_so as number(38,10) )                 as qty_ul_effective_base_fc_so  ,
        cast(qty_ul_promo_total_si as number(38,10) )                       as qty_ul_promo_total_si  ,
        cast(qty_ul_promo_total_so as number(38,10) )                       as qty_ul_promo_total_so  ,
        cast(qty_ul_cannib_loss_si as number(38,10) )                       as qty_ul_cannib_loss_si  ,
        cast(qty_ul_cannib_loss_so as number(38,10) )                       as qty_ul_cannib_loss_so  ,
        cast(qty_ul_pp_dip_si as number(38,10) )                            as qty_ul_pp_dip_si  ,
        cast(qty_ul_pp_dip_so as number(38,10) )                            as qty_ul_pp_dip_so  ,
        cast(qty_ul_total_si as number(38,10) )                             as qty_ul_total_si  ,
        cast(qty_ul_total_so as number(38,10) )                             as qty_ul_total_so  ,
        cast(qty_ul_si_actual as number(38,10) )                            as qty_ul_si_actual  ,
        cast(qty_ul_so_actual as number(38,10) )                            as qty_ul_so_actual  ,
        cast(qty_ul_total_adjust_si as number(38,10) )                      as qty_ul_total_adjust_si  ,
        cast(qty_ul_total_adjust_so as number(38,10) )                      as qty_ul_total_adjust_so  ,
        cast(cust_idx as number(38,0) )                                     as cust_idx  ,
        cast(prod_idx as number(38,0) )                                     as prod_idx  ,
        cast(scen_idx as number(38,0) )                                     as scen_idx  ,
        cast(substring(scen_code,1,255) as text(255) )                      as scen_code  ,
        cast(substring(scen_name,1,255) as text(255) )                      as scen_name  ,
        cast(scenario_guid as text(255) )                                   as scenario_guid  ,
        cast(snapshot_date as date)                                         as snapshot_date  ,
        --cast(substring(sls_wtx_sls_forecast_fact_skey,1,16777216) as text(16777216) ) as sls_wtx_sls_forecast_fact_skey  ,
        {{ dbt_utils.surrogate_key([
            "cast(ltrim(rtrim(upper(substring(source_system,1,255)))) as text(255) ) ",
            "cast(ltrim(rtrim(upper(substring(plan_source_customer_code,1,255)))) as text(255) )",
            "cast(ltrim(rtrim(substring(source_item_identifier,1,255))) as text(255) )",
            "cast(calendar_date as timestamp_ntz(9))",
            "cast(ltrim(rtrim(upper(substring(frozen_forecast,1,255)))) as text(255) )",
            "cast(snapshot_date as timestamp_ntz(9))"
        ]) }}                                                               as unique_key
        

    from tfm
)
    select * from final 
    where (source_system,source_item_identifier) in (select source_system,source_item_identifier from item_master)

