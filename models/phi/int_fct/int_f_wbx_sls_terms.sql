{{
    config(
        on_schema_change="sync_all_columns",
        tags=["sales", "terms","sls_terms"],
        snowflake_warehouse= env_var("DBT_WBX_SF_WH")
        )
}}
with stage as (
    select * from {{ ref('stg_f_wbx_sls_terms')}}
),
snapshot_date as (
    select trunc(snapshot_date,'DD') as snapshot_date 
    from {{ ref('stg_d_wtx_lkp_snapshot_date')}}
    where snapshot_date<>to_date('1000-01-01','YYYY-MM-DD')
),
customer_planning as (
    select trade_type_code from {{ ref('stg_d_wbx_customer_planning')}}
),
source as (
    select 
        stg.source_system	,
        cust_idx	,
        cust_plan_lkp.trade_type_code                               as plan_source_customer_code	,
        null                                                        as customer_address_number_guid	,
        sku_idx	,
        stg.source_item_identifier	,
        {{dbt_utils.surrogate_key(
            ["source_system","source_item_identifier",
            ])}}                                                    as item_guid,
        to_date(to_char(day_idx),'YYYYMMDD')                        as calendar_date,
        snap_lkp.snapshot_date	,
        term_code	,
        term_desc	,
        term_create_datetime	,
        term_created_by	,
        rsa_perc	,
        lump_sum	,
        perc_invoiced_sales	,
        perc_gross_sales	,
        early_settlement_perc	,
        edlp_perc	,
        edlp_case_rate	,
        long_term_promo	,
        rsi_perc	,
        fixed_annual_payment	,
        direct_shopper_marketing	,
        other_direct_payment	,
        other_direct_perc	,
        category_payment	,
        indirect_shopper_marketing	,
        other_indirect_payment	,
        other_indirect_perc	,
        field_marketing	,
        consumer_spend	,
        term_start_date	,
        term_end_date	,
        status_code	,
        status_name	,
        status_verb	,
        impact_option_code	,
        impact_option_name	,
        impact_code	,
        impact_name	,
        impact_option_valvol_percent	,
        impact_option_lump_sum_flag	,
        impact_option_value	,
        impact_option_fin_impact_estimate

    from stage stg
    left join snapshot_date snap_lkp on 1=1
    inner join customer_planning cust_plan_lkp
    on stg.plan_source_customer_code=cust_plan_lkp.trade_type_code
),
final as (
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
    from source
)
select * from final
