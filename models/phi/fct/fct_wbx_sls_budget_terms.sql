{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        tags=["sales_budget","budget","terms"],
        transient=false,
        unique_key="frozen_forecast",
        incremental_strategy="delete+insert",
        snowflake_warehouse=env_var('DBT_WBX_SF_WH'),
        on_schema_change="sync_all_columns",
        full_refresh= false        
    )
}}

/*  The 2 similar models, fct_wbx_sls_budget_promo and fct_wbx_sls_budget_terms are quarterly sales budget related data sets
    that up until now have been loaded with a manual script by Madhur/Mike.  The switch to use Exceedra as the Sales Budgets source has never taken
    place and so the IICS maps were never used.
    This map initially just copies over the historical data to the new model.  Subsequent runs copy the data from the terms history table based 
    on the configuration of the chosen snapshot date from the config table called sls_wtx_budget_scen_xref.
*/

with 
cte_ref as (select * from {{ ref('src_sls_wtx_budget_scen_xref') }}),
cte_fct as (select * from {{ ref('fct_wbx_sls_terms_hist') }}),
cte_frozen_forecast as (select sls_budget_snapshot_date,frozen_forecast from cte_ref
where sls_budget_snapshot_date=(select max(sls_budget_snapshot_date) from cte_ref)
),src_old as(
    select 
        source_system
        ,cust_idx
        ,plan_source_customer_code
        ,cast(customer_address_number_guid as varchar(255)) as customer_address_number_guid
        ,sku_idx
        ,source_item_identifier
        ,item_guid
        ,calendar_date
        ,snapshot_date
        ,term_code
        ,term_desc
        ,term_create_datetime
        ,term_created_by
        ,scen_idx
        ,scen_name
        ,scen_code
        ,scenario_guid
        ,frozen_forecast
        ,rsa_perc
        ,lump_sum
        ,perc_invoiced_sales
        ,perc_gross_sales
        ,early_settlement_perc
        ,edlp_perc
        ,edlp_case_rate
        ,long_term_promo
        ,rsi_perc
        ,fixed_annual_payment
        ,direct_shopper_marketing
        ,other_direct_payment
        ,other_direct_perc
        ,category_payment
        ,indirect_shopper_marketing
        ,other_indirect_payment
        ,other_indirect_perc
        ,field_marketing
        ,consumer_spend
        ,term_start_date
        ,term_end_date
        ,status_code
        ,status_name
        ,status_verb
        ,impact_option_code
        ,impact_option_name
        ,impact_code
        ,impact_name
        ,impact_option_valvol_percent
        ,impact_option_lump_sum_flag
        ,impact_option_value
        ,impact_option_fin_impact_estimate
        ,sls_wtx_budget_terms_fact_skey
 from {{ref('conv_fct_wbx_sls_budget_terms')}}
    {% if check_table_exists( this.schema, this.table ) == 'False' %}
     limit {{env_var('DBT_NO_LIMIT')}} ----------Variable DBT_NO_LIMIT variable is set TO NULL to load everything from conv model if effective currency model is not present.
    {% else %} limit {{env_var('DBT_LIMIT')}}-----Variable DBT_LIMIT variable is set to 0 to load nothing if effective_currency table exist
    {% endif %}
),
src_incremental as 
(
    select         
        source_system
        ,cust_idx
        ,plan_source_customer_code
        ,cast(customer_address_number_guid as varchar(255)) as customer_address_number_guid
        ,sku_idx
        ,source_item_identifier
        ,item_guid
        ,calendar_date
        ,snapshot_date
        ,term_code
        ,term_desc
        ,term_create_datetime
        ,term_created_by
        ,'1' as scen_idx
        ,'Live' as scen_name
        ,'LIVE' as scen_code
        ,{{ dbt_utils.surrogate_key(["source_system", "scen_idx"]) }} as scenario_guid
        ,frct.frozen_forecast as frozen_forecast
        ,rsa_perc
        ,lump_sum
        ,perc_invoiced_sales
        ,perc_gross_sales
        ,early_settlement_perc
        ,edlp_perc
        ,edlp_case_rate
        ,long_term_promo
        ,rsi_perc
        ,fixed_annual_payment
        ,direct_shopper_marketing
        ,other_direct_payment
        ,other_direct_perc
        ,category_payment
        ,indirect_shopper_marketing
        ,other_indirect_payment
        ,other_indirect_perc
        ,field_marketing
        ,consumer_spend
        ,term_start_date
        ,term_end_date
        ,status_code
        ,status_name
        ,status_verb
        ,impact_option_code
        ,impact_option_name
        ,impact_code
        ,impact_name
        ,impact_option_valvol_percent
        ,impact_option_lump_sum_flag
        ,impact_option_value
        ,impact_option_fin_impact_estimate
        ,UNIQUE_KEY as sls_wtx_budget_terms_fact_skey   from cte_fct fct 
        join cte_frozen_forecast frct 
        on fct.snapshot_date = frct.sls_budget_snapshot_date
    --where snapshot_date = (select sls_budget_snapshot_date from src_sls_wtx_budget_scen_xref) 

),
cte_final as 
(
    select * from src_old
    {% if check_table_exists( this.schema, this.table ) == 'True' %}
    union all 
    select * from src_incremental
    {% endif %}
)
select * from cte_final