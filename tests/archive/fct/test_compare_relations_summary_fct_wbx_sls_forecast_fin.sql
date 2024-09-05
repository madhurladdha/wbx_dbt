{{ config(enabled=false, severity="warn", warn_if=">1") }}

with dbt as (
    select * from {{ ref('fct_wbx_sls_forecast_fin')}}
    
),
iics as (
    select * from {{ ref('conv_wtx_sls_forecast_fin')}}
    
),
snapshot as (
    select snapshot_date from {{ ref('stg_d_wtx_lkp_snapshot_date')}}
),
final as (
select 'DBT' as flag,plan_source_customer_code,sum(tot_vol_kg),
sum(tot_vol_ca),sum(ap_net_sales_value),sum(ap_gross_margin_actual),sum(ap_gross_margin_standard),sum(ap_direct_shopper_marketing_pre_adjustment),sum(ap_gross_selling_value_pre_adjustment),
sum(ap_gross_selling_value)
from dbt
where date_trunc(day,snapshot_date) =  (select snap.snapshot_date 
from snapshot snap)
group by plan_source_customer_code

union

select 'IICS' as flag,plan_source_customer_code,sum(tot_vol_kg),
sum(tot_vol_ca),sum(ap_net_sales_value),sum(ap_gross_margin_actual),sum(ap_gross_margin_standard),sum(ap_direct_shopper_marketing_pre_adjustment),sum(ap_gross_selling_value_pre_adjustment),
sum(ap_gross_selling_value)
from iics
where date_trunc(day,snapshot_date) =  (select snap.snapshot_date 
from snapshot snap)
group by plan_source_customer_code,snapshot_date
order by plan_source_customer_code
)
select * from final
