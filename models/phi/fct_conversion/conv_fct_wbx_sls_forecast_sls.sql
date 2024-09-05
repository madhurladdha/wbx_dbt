 
   {{
    config(
         tags = ["ax_hist_fact","ax_hist_sales","ax_hist_on_demand"]
    )
}}

WITH old_fct AS 
(
    SELECT * FROM {{source('FACTS_FOR_COMPARE','sls_wtx_sls_forecast_fact')}} WHERE SOURCE_SYSTEM = '{{env_var("DBT_SOURCE_SYSTEM")}}' and  {{env_var("DBT_PICK_FROM_CONV")}}='Y'
),

converted_fct as (

    select
        cast(substring(source_system,1,255) as text(255) )                  as source_system  ,
        cast(substring(source_item_identifier,1,255) as text(255) )         as source_item_identifier  ,
        cast({{dbt_utils.surrogate_key(["source_system",
        "ltrim(rtrim(source_item_identifier))",])}} as text(255) )          as item_guid  ,
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
        ]) }}                                                           as unique_key

from old_fct
)
select * from converted_fct