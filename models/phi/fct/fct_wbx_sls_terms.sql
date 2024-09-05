{{
  config( 
    materialized=env_var('DBT_MAT_INCREMENTAL'), 
    tags=["sales", "terms","sls_terms"],
    snowflake_warehouse= env_var("DBT_WBX_SF_WH"),
    unique_key='snapshot_date', 
    on_schema_change='sync_all_columns', 
    incremental_strategy='delete+insert',
    full_refresh=false,
    )
}}
/* Approach Used: Static Snapshot w/ Historical Conversion
    The approach used for this table is a Snapshot approach but also requires historical conversion from the old IICS data sets.
    Full details can be found in applicable documentation, but the highlights are provided here.
    1) References the old "conversion" or IICS data set for all snapshots up to the migration date.
    2) Environment variables used to drive the filtering so that the IICS data set is only pulled in on the initial run of the model in a new db/env.
    3) Same variables are used to drive filtering on the new (go-forward) data set
    4) End result should be that all old snapshots are captured and then this dbt model appends each new snapshot/version date to the data set in the dbt model.
    Other Design features:
    1) Model should NEVER be allowed to full-refresh.  This could wipe out all history.
    2) Model is incremental with unique_key = version date.  This ensures that past version dates are never deleted and re-runs on the same day will simply delete for
        the given version date and reload.
*/

with old_table as
(
    select * from {{ref('conv_fct_wbx_sls_terms')}}  
    {% if check_table_exists( this.schema, this.table ) == 'False' %}
     limit {{env_var('DBT_NO_LIMIT')}} ----------Variable DBT_NO_LIMIT variable is set TO NULL to load everything from conv model if effective currency model is not present.
    {% else %} limit {{env_var('DBT_LIMIT')}}-----Variable DBT_LIMIT variable is set to 0 to load nothing if effective_currency table exist

{% endif %}

),

base_fct  as (
    select * from {{ref('int_f_wbx_sls_terms')}}
    {% if check_table_exists( this.schema, this.table ) == 'True' %}
     limit {{env_var('DBT_NO_LIMIT')}}
    {% else %} limit {{env_var('DBT_LIMIT')}}
    {% endif %}
),
old_model as
(
    select  
        cast(substring(source_system,1,255) as text(255) )                as source_system  ,
        cast(cust_idx as number(38,0) )                                   as cust_idx  ,
        cast(substring(plan_source_customer_code,1,255) as text(255) )    as plan_source_customer_code  ,
        cast(customer_address_number_guid as text(255) )                  as customer_address_number_guid  ,
        cast(sku_idx as number(38,0) )                                    as sku_idx  ,
        cast(substring(source_item_identifier,1,255) as text(255) )       as source_item_identifier  ,
        cast(item_guid as text(255) )                                     as item_guid  ,
        cast(calendar_date as date)                                       as calendar_date  ,
        cast(snapshot_date as date)                                       as snapshot_date  ,
        cast(substring(term_code,1,255) as text(255) )                    as term_code  ,
        cast(substring(term_desc,1,255) as text(255) )                    as term_desc  ,
        cast(term_create_datetime as timestamp_ntz(9) )                   as term_create_datetime  ,
        cast(substring(term_created_by,1,255) as text(255) )              as term_created_by  ,
        cast(rsa_perc as float)                                           as rsa_perc  ,
        cast(lump_sum as float)                                           as lump_sum  ,
        cast(perc_invoiced_sales as float)                                as perc_invoiced_sales  ,
        cast(perc_gross_sales as float)                                   as perc_gross_sales  ,
        cast(early_settlement_perc as float)                              as early_settlement_perc  ,
        cast(edlp_perc as float)                                          as edlp_perc  ,
        cast(edlp_case_rate as float)                                     as edlp_case_rate  ,
        cast(long_term_promo as float)                                    as long_term_promo  ,
        cast(rsi_perc as float)                                           as rsi_perc  ,
        cast(fixed_annual_payment as float)                               as fixed_annual_payment  ,
        cast(direct_shopper_marketing as float)                           as direct_shopper_marketing  ,
        cast(other_direct_payment as float)                               as other_direct_payment  ,
        cast(other_direct_perc as float)                                  as other_direct_perc  ,
        cast(category_payment as float)                                   as category_payment  ,
        cast(indirect_shopper_marketing as float)                         as indirect_shopper_marketing  ,
        cast(other_indirect_payment as float)                             as other_indirect_payment  ,
        cast(other_indirect_perc as float)                                as other_indirect_perc  ,
        cast(field_marketing as float)                                    as field_marketing  ,
        cast(consumer_spend as float)                                     as consumer_spend  ,
        cast(term_start_date as date)                                     as term_start_date  ,
        cast(term_end_date as date)                                       as term_end_date  ,
        cast(substring(status_code,1,255) as text(255) )                  as status_code  ,
        cast(substring(status_name,1,255) as text(255) )                  as status_name  ,
        cast(substring(status_verb,1,255) as text(255) )                  as status_verb  ,
        cast(substring(impact_option_code,1,255) as text(255) )           as impact_option_code  ,
        cast(substring(impact_option_name,1,255) as text(255) )           as impact_option_name  ,
        cast(substring(impact_code,1,255) as text(255) )                  as impact_code  ,
        cast(substring(impact_name,1,255) as text(255) )                  as impact_name  ,
        cast(substring(impact_option_valvol_percent,1,255) as text(255) ) as impact_option_valvol_percent  ,
        cast(substring(impact_option_lump_sum_flag,1,10) as text(10) )    as impact_option_lump_sum_flag  ,
        cast(impact_option_value as float)                                as impact_option_value  ,
        cast(impact_option_fin_impact_estimate as float)                  as impact_option_fin_impact_estimate  ,
       -- cast(substring(sls_wtx_terms_fact_skey,1,64) as text(64) )        as sls_wtx_terms_fact_skey  ,
        cast(unique_key as text(255) )                                    as unique_key
from old_table
),
snpt_fact as (
    select  
        cast(substring(source_system,1,255) as text(255) )                as source_system  ,
        cast(cust_idx as number(38,0) )                                   as cust_idx  ,
        cast(substring(plan_source_customer_code,1,255) as text(255) )    as plan_source_customer_code  ,
        cast(customer_address_number_guid as text(255) )                  as customer_address_number_guid  ,
        cast(sku_idx as number(38,0) )                                    as sku_idx  ,
        cast(substring(source_item_identifier,1,255) as text(255) )       as source_item_identifier  ,
        cast(item_guid as text(255) )                                     as item_guid  ,
        cast(calendar_date as date)                                       as calendar_date  ,
        cast(snapshot_date as date)                                       as snapshot_date  ,
        cast(substring(term_code,1,255) as text(255) )                    as term_code  ,
        cast(substring(term_desc,1,255) as text(255) )                    as term_desc  ,
        cast(term_create_datetime as timestamp_ntz(9) )                   as term_create_datetime  ,
        cast(substring(term_created_by,1,255) as text(255) )              as term_created_by  ,
        cast(rsa_perc as float)                                           as rsa_perc  ,
        cast(lump_sum as float)                                           as lump_sum  ,
        cast(perc_invoiced_sales as float)                                as perc_invoiced_sales  ,
        cast(perc_gross_sales as float)                                   as perc_gross_sales  ,
        cast(early_settlement_perc as float)                              as early_settlement_perc  ,
        cast(edlp_perc as float)                                          as edlp_perc  ,
        cast(edlp_case_rate as float)                                     as edlp_case_rate  ,
        cast(long_term_promo as float)                                    as long_term_promo  ,
        cast(rsi_perc as float)                                           as rsi_perc  ,
        cast(fixed_annual_payment as float)                               as fixed_annual_payment  ,
        cast(direct_shopper_marketing as float)                           as direct_shopper_marketing  ,
        cast(other_direct_payment as float)                               as other_direct_payment  ,
        cast(other_direct_perc as float)                                  as other_direct_perc  ,
        cast(category_payment as float)                                   as category_payment  ,
        cast(indirect_shopper_marketing as float)                         as indirect_shopper_marketing  ,
        cast(other_indirect_payment as float)                             as other_indirect_payment  ,
        cast(other_indirect_perc as float)                                as other_indirect_perc  ,
        cast(field_marketing as float)                                    as field_marketing  ,
        cast(consumer_spend as float)                                     as consumer_spend  ,
        cast(term_start_date as date)                                     as term_start_date  ,
        cast(term_end_date as date)                                       as term_end_date  ,
        cast(substring(status_code,1,255) as text(255) )                  as status_code  ,
        cast(substring(status_name,1,255) as text(255) )                  as status_name  ,
        cast(substring(status_verb,1,255) as text(255) )                  as status_verb  ,
        cast(substring(impact_option_code,1,255) as text(255) )           as impact_option_code  ,
        cast(substring(impact_option_name,1,255) as text(255) )           as impact_option_name  ,
        cast(substring(impact_code,1,255) as text(255) )                  as impact_code  ,
        cast(substring(impact_name,1,255) as text(255) )                  as impact_name  ,
        cast(substring(impact_option_valvol_percent,1,255) as text(255) ) as impact_option_valvol_percent  ,
        cast(substring(impact_option_lump_sum_flag,1,10) as text(10) )    as impact_option_lump_sum_flag  ,
        cast(impact_option_value as float)                                as impact_option_value  ,
        cast(impact_option_fin_impact_estimate as float)                  as impact_option_fin_impact_estimate  ,
       -- cast(substring(sls_wtx_terms_fact_skey,1,64) as text(64) )        as sls_wtx_terms_fact_skey  ,
        cast(unique_key as text(255) )                                    as unique_key
from base_fct bf
    
)

select * from snpt_fact
union
select * from old_model