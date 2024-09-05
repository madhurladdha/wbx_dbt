{{
    config(
        tags = ["sls","sales","forecast","sls_forecast"]
    )
}}

with fact as (
    select * from {{ ref('src_exc_fct_volume_cache')}}
),
customer as (
    select * from {{ ref('src_exc_dim_pc_customer')}}
),
product as (
    select * from {{ ref('src_exc_dim_pc_product')}}
),
scenario as (
    select * from {{ ref('src_exc_dim_scenario')}}
),
snapshot_date as (
   select snapshot_Date from (
   select *,rank() over (order by snapshot_Date desc) rnknum 
   from {{ ref('stg_d_wtx_lkp_snapshot_date')}} ) where rnknum=1
),
source as (
    
        select 
            '{{env_var("DBT_SOURCE_SYSTEM")}}'      as source_system
            ,prod.code                              as source_item_identifier 
            ,cust.code                              as plan_source_customer_code
            ,to_date(to_char(day_idx),'YYYYMMDD')   as calendar_date
            ,scen_code                              as frozen_forecast
            ,isonpromo_si                           as isonpromo_si
            ,isonpromo_so                           as isonpromo_so
            ,ispreorpostpromo_si                    as ispreorpostpromo_si
            ,ispreorpostpromo_so                    as ispreorpostpromo_so
            ,listingactive                          as listingactive
            ,total_baseretentionpercentage          as total_baseretentionpercentage
            ,total_si_preorpostdippercentage        as total_si_preorpostdippercentage
            ,total_so_preorpostdippercentage        as total_so_preorpostdippercentage
            ,is_vol_total_nonzero                   as is_vol_total_nonzero
            ,vol_stat_base_fc_si                    as qty_ca_stat_base_fc_si
            ,vol_stat_base_fc_so                    as qty_ca_stat_base_fc_so
            ,vol_override_si                        as qty_ca_override_si
            ,vol_override_so                        as qty_ca_override_so
            ,vol_effective_base_fc_si               as qty_ca_effective_base_fc_si
            ,vol_effective_base_fc_so               as qty_ca_effective_base_fc_so
            ,vol_promo_total_si                     as qty_ca_promo_total_si
            ,vol_promo_total_so                     as qty_ca_promo_total_so
            ,vol_cannib_loss_si                     as qty_ca_cannib_loss_si
            ,vol_cannib_loss_so                     as qty_ca_cannib_loss_so
            ,vol_pp_dip_si                          as qty_ca_pp_dip_si
            ,vol_pp_dip_so                          as qty_ca_pp_dip_so
            ,vol_total_si                           as qty_ca_total_si
            ,vol_total_so                           as qty_ca_total_so
            ,vol_si_actual                          as qty_ca_si_actual
            ,vol_so_actual                          as qty_ca_so_actual
            ,vol_total_adjust_si                    as qty_ca_total_adjust_si
            ,vol_total_adjust_so                    as qty_ca_total_adjust_so
            ,cust.idx                               as cust_idx
            ,prod.idx                               as prod_idx
            ,scen.scen_idx                          as scen_idx
            ,scen.scen_name                         as scen_name
            ,scen.scen_code                         as scen_code
        from fact 
        left join customer cust
        on fact.cust_idx = cust.idx
        left join product prod
        on fact.sku_idx = prod.idx
        left join scenario scen
        on fact.scen_idx = scen.scen_idx
        left join snapshot_date snapshot on 1=1
        where scen.scen_code = 'LIVE'
        and to_date(to_char(day_idx),'YYYYMMDD') >= date_trunc('month', snapshot.snapshot_date)
),
final as (
    select
        source_system,
	    source_item_identifier,
	    plan_source_customer_code,
	    calendar_date,
	    frozen_forecast,
	    isonpromo_si,
	    isonpromo_so,
	    ispreorpostpromo_si,
	    ispreorpostpromo_so,
	    listingactive,
	    total_baseretentionpercentage,
	    total_si_preorpostdippercentage,
	    total_so_preorpostdippercentage,
	    is_vol_total_nonzero,
	    qty_ca_stat_base_fc_si,
	    qty_ca_stat_base_fc_so,
	    qty_ca_override_si,
	    qty_ca_override_so,
	    qty_ca_effective_base_fc_si,
	    qty_ca_effective_base_fc_so,
	    qty_ca_promo_total_si,
	    qty_ca_promo_total_so,
	    qty_ca_cannib_loss_si,
	    qty_ca_cannib_loss_so,
	    qty_ca_pp_dip_si,
	    qty_ca_pp_dip_so,
	    qty_ca_total_si,
	    qty_ca_total_so,
	    qty_ca_si_actual,
	    qty_ca_so_actual,
	    qty_ca_total_adjust_si,
	    qty_ca_total_adjust_so,
	    cust_idx,
	    prod_idx,
	    scen_idx,
	    scen_name,
	    scen_code
    from source
)
select * from final