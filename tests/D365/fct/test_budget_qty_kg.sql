{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

with aggregate as (
    select
        '1' as seq,
        SUM(budkgs) as total
    from {{ ref('src_wbx_ibe_budget_sku_pl') }}

    group by 1

    union

    select
        '2',
        ROUND(SUM(budget_qty_kg), 2)

    from {{ ref('int_f_wbx_sls_ibe_budget_fin') }}
    where

        frozen_forecast
        in (select distinct frozen_forecast from {{ ref('src_wbx_ibe_budget_sku_pl') }}
        ) and budget_qty_kg <> '0'
    group by 1

    union

    select
        '3',
        ROUND(SUM(fcf_tot_vol_kg), 2)

    from {{ ref('fct_wbx_sls_ibe_budget') }}
    where

        frozen_forecast in (select distinct frozen_forecast from {{ ref('src_wbx_ibe_budget_sku_pl') }})
    and fcf_tot_vol_kg <> '0'
    group by 1

    union

    select
        '4',
        ROUND(SUM(fcf_tot_vol_kg), 2)
    from {{ ref('fct_wbx_sls_uber') }} as ub

    left join
        (select

            source_system,
            source_item_identifier,
            MAX(dummy_product_flag) as dummy_product_flag,
            MAX(item_type) as item_type,
            MAX(branding_desc) as branding_desc,
            MAX(product_class_desc) as product_class_desc,
            MAX(sub_product_desc) as sub_product_desc,

            MAX(strategic_desc) as strategic_desc,

            MAX(power_brand_desc) as power_brand_desc,

            MAX(manufacturing_group_desc) as manufacturing_group_desc,

            MAX(category_desc) as category_desc,

            MAX(pack_size_desc) as pack_size_desc,

            MAX(sub_category_desc) as sub_category_desc,

            MAX(consumer_units_in_trade_units) as consumer_units_in_trade_units,

            MAX(promo_type_desc) as promo_type_desc,

            MAX(consumer_units) as consumer_units,

            MAX(description) as description

        from {{ ref('dim_wbx_item_ext') }}

        group by source_system, source_item_identifier) as itm_ext

        on
            ub.source_system = itm_ext.source_system

            and ub.source_item_identifier = itm_ext.source_item_identifier
    where frozen_forecast = 'F1 2024' and fcf_tot_vol_kg <> 0
    group by 1

    union

    select
        '5',
        ROUND(SUM(fcf_tot_vol_kg), 2)

    from {{ ref('v_sls_wtx_performance_agg') }}

    where
    frozen_forecast in (select distinct frozen_forecast from {{ ref('src_wbx_ibe_budget_sku_pl') }}) and fcf_tot_vol_kg <> 0
    group by 1
),
total as 
(
select 1 as seq,sum(total)/5 all_total from aggregate
)
select total,all_total from total a join aggregate b 
on a.seq = b.seq
where (total-all_total)>0.01