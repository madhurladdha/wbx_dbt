 
   {{
    config(
        tags=["ax_hist_fact","ax_hist_sales","ax_hist_on_demand"],
        snowflake_warehouse= env_var("DBT_WBX_SF_WH")
    )
}}

with
    old_fct as (

        select *
        from {{ source("FACTS_FOR_COMPARE", "sls_wtx_terms_fact") }}
        where source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}' and  {{env_var("DBT_PICK_FROM_CONV")}}='Y'

    ),
converted_fct as (
    select  
        cast(substring(source_system,1,255) as text(255) )                as source_system  ,
        cast(cust_idx as number(38,0) )                                   as cust_idx  ,
        cast(substring(plan_source_customer_code,1,255) as text(255) )    as plan_source_customer_code  ,
        cast(customer_address_number_guid as text(255) )                  as customer_address_number_guid  ,
        cast(sku_idx as number(38,0) )                                    as sku_idx  ,
        cast(substring(source_item_identifier,1,255) as text(255) )       as source_item_identifier  ,
        {{dbt_utils.surrogate_key(
            ["source_system","source_item_identifier",
            ])}}                                                          as item_guid  ,
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
        cast(ltrim(rtrim(impact_option_fin_impact_estimate)) as float)    as impact_option_fin_impact_estimate  ,
       -- cast(substring(sls_wtx_terms_fact_skey,1,64) as text(64) )        as sls_wtx_terms_fact_skey  ,
          {{ dbt_utils.surrogate_key([
            "cast(ltrim(rtrim(upper(substring(source_system,1,255)))) as text(255))",
            "cast(ltrim(rtrim(substring(plan_source_customer_code,1,255))) as text(255))",
            "cast(ltrim(rtrim(substring(source_item_identifier,1,255))) as text(255))",
            "cast(ltrim(rtrim(substring(term_code,1,255))) as text(255))",
            "cast(calendar_date as timestamp_ntz(9))",
            "cast(snapshot_date as timestamp_ntz(9))"
        ]) }}                                                             as unique_key
    from old_fct
)
select * from converted_fct