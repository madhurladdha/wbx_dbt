{{ config( 
  snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
  tags=["sales", "terms","sls_terms"]
) }} 
with sls_terms_fact as (
    select * from {{ ref('fct_wbx_sls_terms')}}
),
sls_budget_fact as (
    select * from {{ ref('fct_wbx_sls_budget_terms')}}
),
fc_snapshot as (
    select * from {{ ref('dim_wbx_fc_snapshot')}}
),
dim_date as (
    select * from {{ ref('src_dim_date')}}
),
planning_date_oc as (
    select * from {{ ref('dim_wbx_planning_date_oc')}}
),
customer_planning as (
    select * from {{ ref('dim_wbx_cust_planning')}} where COMPANY_CODE='WBX'
),
budget_scen_xref as (
    select * from {{ ref('src_sls_wtx_budget_scen_xref')}}
    where current_version_flag=1
),
item_ext as (
    select
        source_system                       as source_system,
        item_guid                           as item_guid,
        max(item_type)                      as item_type,
        max(branding_desc)                  as branding_desc,
        max(product_class_desc)             as product_class_desc,
        max(sub_product_desc)               as sub_product_desc,
        max(strategic_desc)                 as strategic_desc,
        max(power_brand_desc)               as power_brand_desc,
        max(manufacturing_group_desc)       as manufacturing_group_desc,
        max(category_desc)                  as category_desc,
        max(pack_size_desc)                 as pack_size_desc,
        max(sub_category_desc)              as sub_category_desc,
        max(consumer_units_in_trade_units)  as consumer_units_in_trade_units,
        max(promo_type_desc)                as promo_type_desc,
        max(consumer_units)                 as consumer_units,
        max(description)                    as description
    from {{ ref('dim_wbx_item_ext')}}
    group by source_system, item_guid
),
terms_fc_snapshot as (
            (select snapshot_date
            from
            (select *,
            rank() over (partition by source_system,snapshot_date,snapshot_model,snapshot_code
                        order by snapshot_type asc) rank_no
            from fc_snapshot
            where snapshot_type = 'WEEK_END') where rank_no = 1)
            union
            select max(snapshot_date)
            from sls_terms_fact
),
source1 as (
    select
        terms_fact.source_system,
        source_item_identifier,
        trim(plan_source_customer_code) as plan_source_customer_code,
        terms_fact.calendar_date,
        terms_fact.snapshot_date,
        'DAILY' as frozen_forecast,
        'TERMS' as source_content_filter,
        terms_fact.term_code,
        terms_fact.term_desc,
        terms_fact.term_create_datetime,
        terms_fact.term_created_by,
        terms_fact.rsa_perc,
        terms_fact.lump_sum,
        terms_fact.perc_invoiced_sales,
        terms_fact.perc_gross_sales,
        terms_fact.early_settlement_perc,
        terms_fact.edlp_perc,
        terms_fact.edlp_case_rate,
        terms_fact.long_term_promo,
        terms_fact.rsi_perc,
        terms_fact.fixed_annual_payment,
        terms_fact.direct_shopper_marketing,
        terms_fact.other_direct_payment,
        terms_fact.other_direct_perc,
        terms_fact.category_payment,
        terms_fact.indirect_shopper_marketing,
        terms_fact.other_indirect_payment,
        terms_fact.other_indirect_perc,
        terms_fact.field_marketing,
        terms_fact.consumer_spend,
        terms_fact.term_start_date,
        terms_fact.term_end_date,
        terms_fact.status_code,
        terms_fact.status_name,
        terms_fact.status_verb,
        terms_fact.impact_option_code,
        terms_fact.impact_option_name,
        terms_fact.impact_code,
        terms_fact.impact_name,
        terms_fact.impact_option_valvol_percent,
        terms_fact.impact_option_lump_sum_flag,
        terms_fact.impact_option_value,
        terms_fact.impact_option_fin_impact_estimate,
        plan.market as market,
        plan.sub_market as submarket,
        plan.trade_class as trade_class,
        plan.trade_group as trade_group,
        plan.trade_type as trade_type,
        plan.trade_sector_desc as trade_sector,
        itm_ext.description,
        itm_ext.item_type,
        itm_ext.branding_desc,
        itm_ext.product_class_desc,
        itm_ext.sub_product_desc,
        itm_ext.strategic_desc,
        itm_ext.power_brand_desc,
        itm_ext.manufacturing_group_desc,
        itm_ext.category_desc,
        itm_ext.pack_size_desc,
        itm_ext.sub_category_desc,
        dt.report_fiscal_year,
        dt.report_fiscal_year_period_no,
        dt.fiscal_year_begin_dt,
        dt.fiscal_year_end_dt,
        dtp.planning_week_code,
        dtp.planning_week_start_dt,
        dtp.planning_week_end_dt,
        dtp.planning_week_no,
        dtp.planning_month_code,
        dtp.planning_month_start_dt,
        dtp.planning_month_end_dt,
        dtp.planning_quarter_no,
        dtp.planning_quarter_start_dt,
        dtp.planning_quarter_end_dt,
        dtp.planning_year_no,
        dtp.planning_year_start_dt,
        dtp.planning_year_end_dt
    from sls_terms_fact terms_fact
         inner join terms_fc_snapshot d        
         on terms_fact.snapshot_date = d.snapshot_date
         left join dim_date dt on terms_fact.calendar_date = dt.calendar_date
         left join planning_date_oc dtp
         on terms_fact.source_system = dtp.source_system
         and terms_fact.calendar_date = dtp.calendar_date
        left join item_ext itm_ext
        on terms_fact.source_system = itm_ext.source_system
        and terms_fact.item_guid = itm_ext.item_guid
    left join
        customer_planning plan
        on trim(terms_fact.plan_source_customer_code) = trim(plan.trade_type_code)
),
source2 as (
    select  
        budget_terms_fact.source_system,
        source_item_identifier,
        trim(plan_source_customer_code) as plan_source_customer_code,
        budget_terms_fact.calendar_date,
        budget_terms_fact.snapshot_date,
        budget_terms_fact.frozen_forecast,
        'BUDGET_TERMS' as source_content_filter,
        budget_terms_fact.term_code,
        budget_terms_fact.term_desc,
        budget_terms_fact.term_create_datetime,
        budget_terms_fact.term_created_by,
        budget_terms_fact.rsa_perc,
        budget_terms_fact.lump_sum,
        budget_terms_fact.perc_invoiced_sales,
        budget_terms_fact.perc_gross_sales,
        budget_terms_fact.early_settlement_perc,
        budget_terms_fact.edlp_perc,
        budget_terms_fact.edlp_case_rate,
        budget_terms_fact.long_term_promo,
        budget_terms_fact.rsi_perc,
        budget_terms_fact.fixed_annual_payment,
        budget_terms_fact.direct_shopper_marketing,
        budget_terms_fact.other_direct_payment,
        budget_terms_fact.other_direct_perc,
        budget_terms_fact.category_payment,
        budget_terms_fact.indirect_shopper_marketing,
        budget_terms_fact.other_indirect_payment,
        budget_terms_fact.other_indirect_perc,
        budget_terms_fact.field_marketing,
        budget_terms_fact.consumer_spend,
        budget_terms_fact.term_start_date,
        budget_terms_fact.term_end_date,
        budget_terms_fact.status_code,
        budget_terms_fact.status_name,
        budget_terms_fact.status_verb,
        budget_terms_fact.impact_option_code,
        budget_terms_fact.impact_option_name,
        budget_terms_fact.impact_code,
        budget_terms_fact.impact_name,
        budget_terms_fact.impact_option_valvol_percent,
        budget_terms_fact.impact_option_lump_sum_flag,
        budget_terms_fact.impact_option_value,
        budget_terms_fact.impact_option_fin_impact_estimate,
        plan.market as market,
        plan.sub_market as submarket,
        plan.trade_class as trade_class,
        plan.trade_group as trade_group,
        plan.trade_type as trade_type,
        plan.trade_sector_desc as trade_sector,
        itm_ext.description,
        itm_ext.item_type, 
        itm_ext.branding_desc, 
        itm_ext.product_class_desc, 
        itm_ext.sub_product_desc, 
        itm_ext.strategic_desc, 
        itm_ext.power_brand_desc, 
        itm_ext.manufacturing_group_desc, 
        itm_ext.category_desc, 
        itm_ext.pack_size_desc, 
        itm_ext.sub_category_desc,
        dt.report_fiscal_year,
        dt.report_fiscal_year_period_no,
        dt.fiscal_year_begin_dt,
        dt.fiscal_year_end_dt,
        dtp.planning_week_code,
        dtp.planning_week_start_dt,
        dtp.planning_week_end_dt,
        dtp.planning_week_no,
        dtp.planning_month_code,
        dtp.planning_month_start_dt,
        dtp.planning_month_end_dt,
        dtp.planning_quarter_no,
        dtp.planning_quarter_start_dt,
        dtp.planning_quarter_end_dt,
        dtp.planning_year_no,
        dtp.planning_year_start_dt,
        dtp.planning_year_end_dt
    from sls_budget_fact budget_terms_fact 
    left join dim_date dt
    on budget_terms_fact.calendar_date = dt.calendar_date
    left join planning_date_oc dtp
    on budget_terms_fact.source_system = dtp.source_system
    and budget_terms_fact.calendar_date = dtp.calendar_date
    left join item_ext itm_ext
    on budget_terms_fact.source_system = itm_ext.source_system
    and budget_terms_fact.item_guid = itm_ext.item_guid
    left join customer_planning plan
    on trim(budget_terms_fact.plan_source_customer_code) = trim(plan.trade_type_code)
    inner join budget_scen_xref scen
    on budget_terms_fact.frozen_forecast=scen.frozen_forecast
),
final as (
    select * from source1
    union all
    select * from source2
)
    select
        source_system,
	    source_item_identifier,
	    plan_source_customer_code,
	    calendar_date,
	    snapshot_date,
	    frozen_forecast,
	    source_content_filter,
	    term_code,
	    term_desc,
	    term_create_datetime,
	    term_created_by,
	    rsa_perc,
	    lump_sum,
	    perc_invoiced_sales,
	    perc_gross_sales,
	    early_settlement_perc,
	    edlp_perc,
	    edlp_case_rate,
	    long_term_promo,
	    rsi_perc,
	    fixed_annual_payment,
	    direct_shopper_marketing,
	    other_direct_payment,
	    other_direct_perc,
	    category_payment,
	    indirect_shopper_marketing,
	    other_indirect_payment,
	    other_indirect_perc,
	    field_marketing,
	    consumer_spend,
	    term_start_date,
	    term_end_date,
	    status_code,
	    status_name,
	    status_verb,
	    impact_option_code,
	    impact_option_name,
	    impact_code,
	    impact_name,
	    impact_option_valvol_percent,
	    impact_option_lump_sum_flag,
	    impact_option_value,
	    impact_option_fin_impact_estimate,
	    market,
	    submarket,
	    trade_class,
	    trade_group,
	    trade_type,
	    trade_sector,
	    description,
	    item_type,
	    branding_desc,
	    product_class_desc,
	    sub_product_desc,
	    strategic_desc,
	    power_brand_desc,
	    manufacturing_group_desc,
	    category_desc,
	    pack_size_desc,
	    sub_category_desc,
	    report_fiscal_year,
	    report_fiscal_year_period_no,
	    fiscal_year_begin_dt,
	    fiscal_year_end_dt,
	    planning_week_code,
	    planning_week_start_dt,
	    planning_week_end_dt,
	    planning_week_no,
	    planning_month_code,
	    planning_month_start_dt,
	    planning_month_end_dt,
	    planning_quarter_no,
	    planning_quarter_start_dt,
	    planning_quarter_end_dt,
	    planning_year_no,
	    planning_year_start_dt,
	    planning_year_end_dt 
    from final
