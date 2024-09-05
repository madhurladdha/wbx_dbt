/*changing the test to manual to check the rounding issue for the column IMPACT_OPTION_FIN_IMPACT_ESTIMATE*/

{{ config( 
  enabled=false, 
  snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
  severity = 'warn', 
  warn_if = '>0' ,
  tags=["sales", "terms","sls_terms","sales_budget","budget"]

) }} 

with a as (

    
select
  
    "SOURCE_SYSTEM"           ,      
    "SOURCE_ITEM_IDENTIFIER"           ,      
    "PLAN_SOURCE_CUSTOMER_CODE"           ,      
    "CALENDAR_DATE"           ,      
    "SNAPSHOT_DATE"           ,      
    "FROZEN_FORECAST"           ,      
    "SOURCE_CONTENT_FILTER"           ,      
    "TERM_CODE"           ,      
    "TERM_DESC"           ,      
    "TERM_CREATE_DATETIME"           ,      
    "TERM_CREATED_BY"           ,      
    "RSA_PERC"           ,      
    "LUMP_SUM"           ,      
    "PERC_INVOICED_SALES"           ,      
    "PERC_GROSS_SALES"           ,      
    "EARLY_SETTLEMENT_PERC"           ,      
    "EDLP_PERC"           ,      
    "EDLP_CASE_RATE"           ,      
    "LONG_TERM_PROMO"           ,      
    "RSI_PERC"           ,      
    "FIXED_ANNUAL_PAYMENT"           ,      
    "DIRECT_SHOPPER_MARKETING"           ,      
    "OTHER_DIRECT_PAYMENT"           ,      
    "OTHER_DIRECT_PERC"           ,      
    "CATEGORY_PAYMENT"           ,      
    "INDIRECT_SHOPPER_MARKETING"           ,      
    "OTHER_INDIRECT_PAYMENT"           ,      
    "OTHER_INDIRECT_PERC"           ,      
    "FIELD_MARKETING"           ,      
    "CONSUMER_SPEND"           ,      
    "TERM_START_DATE"           ,      
    "TERM_END_DATE"           ,      
    "STATUS_CODE"           ,      
    "STATUS_NAME"           ,      
    "STATUS_VERB"           ,      
    "IMPACT_OPTION_CODE"           ,      
    "IMPACT_OPTION_NAME"           ,      
    "IMPACT_CODE"           ,      
    "IMPACT_NAME"           ,      
    "IMPACT_OPTION_VALVOL_PERCENT"           ,      
    "IMPACT_OPTION_LUMP_SUM_FLAG"           ,      
    "IMPACT_OPTION_VALUE"           ,      
     round("IMPACT_OPTION_FIN_IMPACT_ESTIMATE" ,5) as "IMPACT_OPTION_FIN_IMPACT_ESTIMATE"           ,      
    "MARKET"           ,      
    "SUBMARKET"           ,      
    "TRADE_CLASS"           ,      
    "TRADE_GROUP"           ,      
    "TRADE_TYPE"           ,      
    "TRADE_SECTOR"           ,      
    "DESCRIPTION"           ,      
    "ITEM_TYPE"           ,      
    "BRANDING_DESC"           ,      
    "PRODUCT_CLASS_DESC"           ,      
    "SUB_PRODUCT_DESC"           ,      
    "STRATEGIC_DESC"           ,      
    "POWER_BRAND_DESC"           ,      
    "MANUFACTURING_GROUP_DESC"           ,      
    "CATEGORY_DESC"           ,      
    "PACK_SIZE_DESC"           ,      
    "SUB_CATEGORY_DESC"           ,      
    "REPORT_FISCAL_YEAR"           ,      
    "REPORT_FISCAL_YEAR_PERIOD_NO"           ,      
    "FISCAL_YEAR_BEGIN_DT"           ,      
    "FISCAL_YEAR_END_DT"           ,      
    "PLANNING_WEEK_CODE"           ,      
    "PLANNING_WEEK_START_DT"           ,      
    "PLANNING_WEEK_END_DT"           ,      
    "PLANNING_WEEK_NO"           ,      
    "PLANNING_MONTH_CODE"           ,      
    "PLANNING_MONTH_START_DT"           ,      
    "PLANNING_MONTH_END_DT"           ,      
    "PLANNING_QUARTER_NO"           ,      
    "PLANNING_QUARTER_START_DT"           ,      
    "PLANNING_QUARTER_END_DT"           ,      
    "PLANNING_YEAR_NO"           ,      
    "PLANNING_YEAR_START_DT"           ,      
    "PLANNING_YEAR_END_DT"      
  
from {{ source("FACTS_FOR_COMPARE", "v_sls_wtx_terms_summary") }}

),

b as (

    
select
  
    "SOURCE_SYSTEM"           ,      
    "SOURCE_ITEM_IDENTIFIER"           ,      
    "PLAN_SOURCE_CUSTOMER_CODE"           ,      
    "CALENDAR_DATE"           ,      
    "SNAPSHOT_DATE"           ,      
    "FROZEN_FORECAST"           ,      
    "SOURCE_CONTENT_FILTER"           ,      
    "TERM_CODE"           ,      
    "TERM_DESC"           ,      
    "TERM_CREATE_DATETIME"           ,      
    "TERM_CREATED_BY"           ,      
    "RSA_PERC"           ,      
    "LUMP_SUM"           ,      
    "PERC_INVOICED_SALES"           ,      
    "PERC_GROSS_SALES"           ,      
    "EARLY_SETTLEMENT_PERC"           ,      
    "EDLP_PERC"           ,      
    "EDLP_CASE_RATE"           ,      
    "LONG_TERM_PROMO"           ,      
    "RSI_PERC"           ,      
    "FIXED_ANNUAL_PAYMENT"           ,      
    "DIRECT_SHOPPER_MARKETING"           ,      
    "OTHER_DIRECT_PAYMENT"           ,      
    "OTHER_DIRECT_PERC"           ,      
    "CATEGORY_PAYMENT"           ,      
    "INDIRECT_SHOPPER_MARKETING"           ,      
    "OTHER_INDIRECT_PAYMENT"           ,      
    "OTHER_INDIRECT_PERC"           ,      
    "FIELD_MARKETING"           ,      
    "CONSUMER_SPEND"           ,      
    "TERM_START_DATE"           ,      
    "TERM_END_DATE"           ,      
    "STATUS_CODE"           ,      
    "STATUS_NAME"           ,      
    "STATUS_VERB"           ,      
    "IMPACT_OPTION_CODE"           ,      
    "IMPACT_OPTION_NAME"           ,      
    "IMPACT_CODE"           ,      
    "IMPACT_NAME"           ,      
    "IMPACT_OPTION_VALVOL_PERCENT"           ,      
    "IMPACT_OPTION_LUMP_SUM_FLAG"           ,      
    "IMPACT_OPTION_VALUE"           ,      
     round("IMPACT_OPTION_FIN_IMPACT_ESTIMATE" ,5) as "IMPACT_OPTION_FIN_IMPACT_ESTIMATE"           ,      
    "MARKET"           ,      
    "SUBMARKET"           ,      
    "TRADE_CLASS"           ,      
    "TRADE_GROUP"           ,      
    "TRADE_TYPE"           ,      
    "TRADE_SECTOR"           ,      
    "DESCRIPTION"           ,      
    "ITEM_TYPE"           ,      
    "BRANDING_DESC"           ,      
    "PRODUCT_CLASS_DESC"           ,      
    "SUB_PRODUCT_DESC"           ,      
    "STRATEGIC_DESC"           ,      
    "POWER_BRAND_DESC"           ,      
    "MANUFACTURING_GROUP_DESC"           ,      
    "CATEGORY_DESC"           ,      
    "PACK_SIZE_DESC"           ,      
    "SUB_CATEGORY_DESC"           ,      
    "REPORT_FISCAL_YEAR"           ,      
    "REPORT_FISCAL_YEAR_PERIOD_NO"           ,      
    "FISCAL_YEAR_BEGIN_DT"           ,      
    "FISCAL_YEAR_END_DT"           ,      
    "PLANNING_WEEK_CODE"           ,      
    "PLANNING_WEEK_START_DT"           ,      
    "PLANNING_WEEK_END_DT"           ,      
    "PLANNING_WEEK_NO"           ,      
    "PLANNING_MONTH_CODE"           ,      
    "PLANNING_MONTH_START_DT"           ,      
    "PLANNING_MONTH_END_DT"           ,      
    "PLANNING_QUARTER_NO"           ,      
    "PLANNING_QUARTER_START_DT"           ,      
    "PLANNING_QUARTER_END_DT"           ,      
    "PLANNING_YEAR_NO"           ,      
    "PLANNING_YEAR_START_DT"           ,      
    "PLANNING_YEAR_END_DT"      
  
from {{ ref("v_sls_wtx_terms_summary") }}

),

a_intersect_b as (

    select * from a
    

    intersect


    select * from b

),

a_except_b as (

    select * from a
    

    except


    select * from b

),

b_except_a as (

    select * from b
    

    except


    select * from a

),

all_records as (

    select
        *,
        true as in_a,
        true as in_b
    from a_intersect_b

    union all

    select
        *,
        true as in_a,
        false as in_b
    from a_except_b

    union all

    select
        *,
        false as in_a,
        true as in_b
    from b_except_a

),

final as (
    
    select * from all_records
    where not (in_a and in_b)
    order by  in_a desc, in_b desc

)

select * from final



 