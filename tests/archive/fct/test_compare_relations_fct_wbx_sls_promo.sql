{{ config(enabled=false, severity="warn") }}

/*
{% set old_etl_relation = ref("conv_wbx_sls_promo_fact") %}


{% set dbt_relation = ref("fct_wbx_sls_promo") %}


{{
    ent_dbt_package.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        exclude_columns=["promo_guid", "item_guid", "prm_rpt_customer_guid"],
        primary_key="unique_key",
        summarize=false,
    )
}}
*/






















with a as (

    
select
  
    "SOURCE_SYSTEM"           ,      
    "PROMO_IDX"           ,      
    "CUST_IDX"           ,      
    "PLAN_SOURCE_CUSTOMER_CODE"           ,      
    "CUSTOMER_ADDRESS_NUMBER_GUID"           ,      
    "SKU_IDX"           ,      
    "SOURCE_ITEM_IDENTIFIER"           ,      
    "CALENDAR_DATE"           ,      
    "SNAPSHOT_DATE"           ,      
    "REPORTINGSKU_IDX"           ,      
    "ISPROMOSKU"           ,      
    "ISCANNIBSKU"           ,      
    "ISSI_PREPROMODAY"           ,      
    "ISSI_ONPROMODAY"           ,      
    "ISSI_POSTPROMODAY"           ,      
    "ISSO_PREPROMODAY"           ,      
    "ISSO_ONPROMODAY"           ,      
    "ISSO_POSTPROMODAY"           ,      
    "SI_B_VOL_CSE"           ,      
    "SI_B_VOL_SGL"           ,      
    "SI_A_VOL_CSE"           ,      
    "SI_A_VOL_SGL"           ,      
    "SI_T_VOL_CSE"           ,      
    "SI_T_VOL_SGL"           ,      
    "SI_M_VOL_CSE"           ,      
    "SI_M_VOL_SGL"           ,      
    "SI_I_VOL_CSE"           ,      
    "SI_I_VOL_SGL"           ,      
    "SO_B_VOL_CSE"           ,      
    "SO_B_VOL_SGL"           ,      
    "SO_A_VOL_CSE"           ,      
    "SO_A_VOL_SGL"           ,      
    "SO_T_VOL_CSE"           ,      
    "SO_T_VOL_SGL"           ,      
    "SO_M_VOL_CSE"           ,      
    "SO_M_VOL_SGL"           ,      
    "SO_I_VOL_CSE"           ,      
    "SO_I_VOL_SGL"           ,      
    "SI_CANNIB_VOL_CSE"           ,      
    "SI_CANNIB_VOL_SGL"           ,      
    "SO_CANNIB_VOL_CSE"           ,      
    "SO_CANNIB_VOL_SGL"           ,      
    "SI_CANNIB_BASEVOL_CSE"           ,      
    "SI_CANNIB_BASEVOL_SGL"           ,      
    "SO_CANNIB_BASEVOL_CSE"           ,      
    "SO_CANNIB_BASEVOL_SGL"           ,      
    "SI_CANNIB_LOSS_VOL_CSE"           ,      
    "SI_CANNIB_LOSS_VOL_SGL"           ,      
    "SO_CANNIB_LOSS_VOL_CSE"           ,      
    "SO_CANNIB_LOSS_VOL_SGL"           ,      
    "SI_PREDIP_VOL_CSE"           ,      
    "SI_PREDIP_VOL_SGL"           ,      
    "SI_POSTDIP_VOL_CSE"           ,      
    "SI_POSTDIP_VOL_SGL"           ,      
    "SO_PREDIP_VOL_CSE"           ,      
    "SO_PREDIP_VOL_SGL"           ,      
    "SO_POSTDIP_VOL_CSE"           ,      
    "SO_POSTDIP_VOL_SGL"           ,      
    "SI_PREDIP_BASEVOL_CSE"           ,      
    "SI_PREDIP_BASEVOL_SGL"           ,      
    "SI_POSTDIP_BASEVOL_CSE"           ,      
    "SI_POSTDIP_BASEVOL_SGL"           ,      
    "SO_PREDIP_BASEVOL_CSE"           ,      
    "SO_PREDIP_BASEVOL_SGL"           ,      
    "SO_POSTDIP_BASEVOL_CSE"           ,      
    "SO_POSTDIP_BASEVOL_SGL"           ,      
    "POSTPROMODIPPERCENT_SI"           ,      
    "POSTPROMODIPPERCENT_SO"           ,      
    "PREPROMODIPPERCENT_SI"           ,      
    "PREPROMODIPPERCENT_SO"           ,      
    "ONPROMOPHASINGPERCENT_SI"           ,      
    "ONPROMOPHASINGPERCENT_SO"           ,      
    "ROBFUNDINGREQUIRED"           ,      
    "ACTUALS_TOT_VOL_KG"           ,      
    "ACTUALS_AP_GROSS_SALES_VALUE"           ,      
    "ACTUALS_AP_RANGE_SUPPORT_ALLOWANCE"           ,      
    "ACTUALS_AP_EVERYDAY_LOW_PRICES"           ,      
    "ACTUALS_AP_PERMANENT_DISC"           ,      
    "ACTUALS_AP_OFF_INVOICE_DISC"           ,      
    "ACTUALS_AP_INVOICED_SALES_VALUE"           ,      
    "ACTUALS_AP_EARLY_SETTLEMENT_DISC"           ,      
    "ACTUALS_AP_GROWTH_INCENTIVES"           ,      
    "ACTUALS_AP_NET_SALES_VALUE"           ,      
    "ACTUALS_AP_RETRO"           ,      
    "ACTUALS_AP_AVP_DISC"           ,      
    "ACTUALS_AP_VARIABLE_TRADE"           ,      
    "ACTUALS_AP_PROMO_FIXED_FUNDING"           ,      
    "ACTUALS_AP_RANGE_SUPPORT_INCENTIVES"           ,      
    "ACTUALS_AP_NET_NET_SALES_VALUE"           ,      
    "ACTUALS_AP_DIRECT_SHOPPER_MARKETING"           ,      
    "ACTUALS_AP_OTHER_DIRECT_PAYMENTS"           ,      
    "ACTUALS_AP_INDIRECT_SHOPPER_MARKETING"           ,      
    "ACTUALS_AP_OTHER_INDIRECT_PAYMENTS"           ,      
    "ACTUALS_AP_FIXED_TRADE_CUST_INVOICED"           ,      
    "ACTUALS_AP_TOTAL_TRADE_CUST_INVOICED"           ,      
    "ACTUALS_AP_FIXED_TRADE_NON_CUST_INVOICED"           ,      
    "ACTUALS_AP_TOTAL_TRADE"           ,      
    "ACTUALS_AP_NET_REALISABLE_REVENUE"           ,      
    "ACTUALS_AP_TOT_PRIME_COST_STANDARD"           ,      
    "ACTUALS_AP_GROSS_MARGIN_STANDARD"           ,      
    "ACTUALS_AP_GCAT_STANDARD"           ,      
    "ACTUALS_MANSO_TOT_VOL_KG"           ,      
    "ACTUALS_MANSO_GROSS_SALES_VALUE"           ,      
    "ACTUALS_MANSO_RANGE_SUPPORT_ALLOWANCE"           ,      
    "ACTUALS_MANSO_EVERYDAY_LOW_PRICES"           ,      
    "ACTUALS_MANSO_PERMANENT_DISC"           ,      
    "ACTUALS_MANSO_OFF_INVOICE_DISC"           ,      
    "ACTUALS_MANSO_INVOICED_SALES_VALUE"           ,      
    "ACTUALS_MANSO_EARLY_SETTLEMENT_DISC"           ,      
    "ACTUALS_MANSO_GROWTH_INCENTIVES"           ,      
    "ACTUALS_MANSO_NET_SALES_VALUE"           ,      
    "ACTUALS_MANSO_RETRO"           ,      
    "ACTUALS_MANSO_AVP_DISC"           ,      
    "ACTUALS_MANSO_VARIABLE_TRADE"           ,      
    "ACTUALS_MANSO_PROMO_FIXED_FUNDING"           ,      
    "ACTUALS_MANSO_RANGE_SUPPORT_INCENTIVES"           ,      
    "ACTUALS_MANSO_NET_NET_SALES_VALUE"           ,      
    "ACTUALS_MANSO_DIRECT_SHOPPER_MARKETING"           ,      
    "ACTUALS_MANSO_OTHER_DIRECT_PAYMENTS"           ,      
    "ACTUALS_MANSO_INDIRECT_SHOPPER_MARKETING"           ,      
    "ACTUALS_MANSO_OTHER_INDIRECT_PAYMENTS"           ,      
    "ACTUALS_MANSO_FIXED_TRADE_CUST_INVOICED"           ,      
    "ACTUALS_MANSO_TOTAL_TRADE_CUST_INVOICED"           ,      
    "ACTUALS_MANSO_FIXED_TRADE_NON_CUST_INVOICED"           ,      
    "ACTUALS_MANSO_TOTAL_TRADE"           ,      
    "ACTUALS_MANSO_NET_REALISABLE_REVENUE"           ,      
    "ACTUALS_MANSO_TOT_PRIME_COST_STANDARD"           ,      
    "ACTUALS_MANSO_GROSS_MARGIN_STANDARD"           ,      
    "ACTUALS_MANSO_GCAT_STANDARD"           ,      
    "ACTUALS_RETAIL_TOT_VOL_KG"           ,      
    "ACTUALS_AP_RETAIL_REVENUE_MRRSP"           ,      
    "ACTUALS_AP_RETAIL_REVENUE_RSP"           ,      
    "ACTUALS_AP_RETAIL_REVENUE_NET"           ,      
    "ACTUALS_AP_RETAIL_COST_OF_SALES"           ,      
    "ACTUALS_AP_RETAIL_RETAILER_RETRO_FUNDING"           ,      
    "ACTUALS_AP_RETAIL_MARGIN_EXCL_FIXED_FUNDING"           ,      
    "ACTUALS_AP_RETAIL_PROMO_FIXED_SPEND"           ,      
    "ACTUALS_AP_RETAIL_TOTAL_SPEND"           ,      
    "ACTUALS_AP_RETAIL_MARGIN_INCL_FIXED_FUNDING"           ,      
    "ACTUALS_AP_RETAIL_REVENUE_NET_EXCL_MRRSP"           ,      
    "ACTUALS_AP_RETAIL_REVENUE_NET_EXCL_RSP"           ,      
    "BASE_TOT_VOL_KG"           ,      
    "BASE_AP_GROSS_SALES_VALUE"           ,      
    "BASE_AP_RANGE_SUPPORT_ALLOWANCE"           ,      
    "BASE_AP_EVERYDAY_LOW_PRICES"           ,      
    "BASE_AP_PERMANENT_DISC"           ,      
    "BASE_AP_OFF_INVOICE_DISC"           ,      
    "BASE_AP_INVOICED_SALES_VALUE"           ,      
    "BASE_AP_EARLY_SETTLEMENT_DISC"           ,      
    "BASE_AP_GROWTH_INCENTIVES"           ,      
    "BASE_AP_NET_SALES_VALUE"           ,      
    "BASE_AP_RETRO"           ,      
    "BASE_AP_AVP_DISC"           ,      
    "BASE_AP_VARIABLE_TRADE"           ,      
    "BASE_AP_PROMO_FIXED_FUNDING"           ,      
    "BASE_AP_RANGE_SUPPORT_INCENTIVES"           ,      
    "BASE_AP_NET_NET_SALES_VALUE"           ,      
    "BASE_AP_DIRECT_SHOPPER_MARKETING"           ,      
    "BASE_AP_OTHER_DIRECT_PAYMENTS"           ,      
    "BASE_AP_INDIRECT_SHOPPER_MARKETING"           ,      
    "BASE_AP_OTHER_INDIRECT_PAYMENTS"           ,      
    "BASE_AP_FIXED_TRADE_CUST_INVOICED"           ,      
    "BASE_AP_TOTAL_TRADE_CUST_INVOICED"           ,      
    "BASE_AP_FIXED_TRADE_NON_CUST_INVOICED"           ,      
    "BASE_AP_TOTAL_TRADE"           ,      
    "BASE_AP_NET_REALISABLE_REVENUE"           ,      
    "BASE_AP_TOT_PRIME_COST_STANDARD"           ,      
    "BASE_AP_GROSS_MARGIN_STANDARD"           ,      
    "BASE_AP_GCAT_STANDARD"           ,      
    "BASE_MANSO_TOT_VOL_KG"           ,      
    "BASE_MANSO_GROSS_SALES_VALUE"           ,      
    "BASE_MANSO_RANGE_SUPPORT_ALLOWANCE"           ,      
    "BASE_MANSO_EVERYDAY_LOW_PRICES"           ,      
    "BASE_MANSO_PERMANENT_DISC"           ,      
    "BASE_MANSO_OFF_INVOICE_DISC"           ,      
    "BASE_MANSO_INVOICED_SALES_VALUE"           ,      
    "BASE_MANSO_EARLY_SETTLEMENT_DISC"           ,      
    "BASE_MANSO_GROWTH_INCENTIVES"           ,      
    "BASE_MANSO_NET_SALES_VALUE"           ,      
    "BASE_MANSO_RETRO"           ,      
    "BASE_MANSO_AVP_DISC"           ,      
    "BASE_MANSO_VARIABLE_TRADE"           ,      
    "BASE_MANSO_PROMO_FIXED_FUNDING"           ,      
    "BASE_MANSO_RANGE_SUPPORT_INCENTIVES"           ,      
    "BASE_MANSO_NET_NET_SALES_VALUE"           ,      
    "BASE_MANSO_DIRECT_SHOPPER_MARKETING"           ,      
    "BASE_MANSO_OTHER_DIRECT_PAYMENTS"           ,      
    "BASE_MANSO_INDIRECT_SHOPPER_MARKETING"           ,      
    "BASE_MANSO_OTHER_INDIRECT_PAYMENTS"           ,      
    "BASE_MANSO_FIXED_TRADE_CUST_INVOICED"           ,      
    "BASE_MANSO_TOTAL_TRADE_CUST_INVOICED"           ,      
    "BASE_MANSO_FIXED_TRADE_NON_CUST_INVOICED"           ,      
    "BASE_MANSO_TOTAL_TRADE"           ,      
    "BASE_MANSO_NET_REALISABLE_REVENUE"           ,      
    "BASE_MANSO_TOT_PRIME_COST_STANDARD"           ,      
    "BASE_MANSO_GROSS_MARGIN_STANDARD"           ,      
    "BASE_MANSO_GCAT_STANDARD"           ,      
    "BASE_RETAIL_TOT_VOL_KG"           ,      
    "BASE_AP_RETAIL_REVENUE_MRRSP"           ,      
    "BASE_AP_RETAIL_REVENUE_RSP"           ,      
    "BASE_AP_RETAIL_REVENUE_NET"           ,      
    "BASE_AP_RETAIL_COST_OF_SALES"           ,      
    "BASE_AP_RETAIL_RETAILER_RETRO_FUNDING"           ,      
    "BASE_AP_RETAIL_MARGIN_EXCL_FIXED_FUNDING"           ,      
    "BASE_AP_RETAIL_PROMO_FIXED_SPEND"           ,      
    "BASE_AP_RETAIL_TOTAL_SPEND"           ,      
    "BASE_AP_RETAIL_MARGIN_INCL_FIXED_FUNDING"           ,      
    "BASE_AP_RETAIL_REVENUE_NET_EXCL_MRRSP"           ,      
    "BASE_AP_RETAIL_REVENUE_NET_EXCL_RSP"           ,      
    "FORECAST_TOT_VOL_KG"           ,      
    "FORECAST_AP_GROSS_SALES_VALUE"           ,      
    "FORECAST_AP_RANGE_SUPPORT_ALLOWANCE"           ,      
    "FORECAST_AP_EVERYDAY_LOW_PRICES"           ,      
    "FORECAST_AP_PERMANENT_DISC"           ,      
    "FORECAST_AP_OFF_INVOICE_DISC"           ,      
    "FORECAST_AP_INVOICED_SALES_VALUE"           ,      
    "FORECAST_AP_EARLY_SETTLEMENT_DISC"           ,      
    "FORECAST_AP_GROWTH_INCENTIVES"           ,      
    "FORECAST_AP_NET_SALES_VALUE"           ,      
    "FORECAST_AP_RETRO"           ,      
    "FORECAST_AP_AVP_DISC"           ,      
    "FORECAST_AP_VARIABLE_TRADE"           ,      
    "FORECAST_AP_PROMO_FIXED_FUNDING"           ,      
    "FORECAST_AP_RANGE_SUPPORT_INCENTIVES"           ,      
    "FORECAST_AP_NET_NET_SALES_VALUE"           ,      
    "FORECAST_AP_DIRECT_SHOPPER_MARKETING"           ,      
    "FORECAST_AP_OTHER_DIRECT_PAYMENTS"           ,      
    "FORECAST_AP_INDIRECT_SHOPPER_MARKETING"           ,      
    "FORECAST_AP_OTHER_INDIRECT_PAYMENTS"           ,      
    "FORECAST_AP_FIXED_TRADE_CUST_INVOICED"           ,      
    "FORECAST_AP_TOTAL_TRADE_CUST_INVOICED"           ,      
    "FORECAST_AP_FIXED_TRADE_NON_CUST_INVOICED"           ,      
    "FORECAST_AP_TOTAL_TRADE"           ,      
    "FORECAST_AP_TOTAL_TRADE_GBP"           ,      
    "FORECAST_AP_NET_REALISABLE_REVENUE"           ,      
    "FORECAST_AP_TOT_PRIME_COST_STANDARD"           ,      
    "FORECAST_AP_GROSS_MARGIN_STANDARD"           ,      
    "FORECAST_AP_GCAT_STANDARD"           ,      
    "FORECAST_MANSO_TOT_VOL_KG"           ,      
    "FORECAST_MANSO_GROSS_SALES_VALUE"           ,      
    "FORECAST_MANSO_RANGE_SUPPORT_ALLOWANCE"           ,      
    "FORECAST_MANSO_EVERYDAY_LOW_PRICES"           ,      
    "FORECAST_MANSO_PERMANENT_DISC"           ,      
    "FORECAST_MANSO_OFF_INVOICE_DISC"           ,      
    "FORECAST_MANSO_INVOICED_SALES_VALUE"           ,      
    "FORECAST_MANSO_EARLY_SETTLEMENT_DISC"           ,      
    "FORECAST_MANSO_GROWTH_INCENTIVES"           ,      
    "FORECAST_MANSO_NET_SALES_VALUE"           ,      
    "FORECAST_MANSO_RETRO"           ,      
    "FORECAST_MANSO_AVP_DISC"           ,      
    "FORECAST_MANSO_VARIABLE_TRADE"           ,      
    "FORECAST_MANSO_PROMO_FIXED_FUNDING"           ,      
    "FORECAST_MANSO_RANGE_SUPPORT_INCENTIVES"           ,      
    "FORECAST_MANSO_NET_NET_SALES_VALUE"           ,      
    "FORECAST_MANSO_DIRECT_SHOPPER_MARKETING"           ,      
    "FORECAST_MANSO_OTHER_DIRECT_PAYMENTS"           ,      
    "FORECAST_MANSO_INDIRECT_SHOPPER_MARKETING"           ,      
    "FORECAST_MANSO_OTHER_INDIRECT_PAYMENTS"           ,      
    "FORECAST_MANSO_FIXED_TRADE_CUST_INVOICED"           ,      
    "FORECAST_MANSO_TOTAL_TRADE_CUST_INVOICED"           ,      
    "FORECAST_MANSO_FIXED_TRADE_NON_CUST_INVOICED"           ,      
    "FORECAST_MANSO_TOTAL_TRADE"           ,      
    "FORECAST_MANSO_NET_REALISABLE_REVENUE"           ,      
    "FORECAST_MANSO_TOT_PRIME_COST_STANDARD"           ,      
    "FORECAST_MANSO_GROSS_MARGIN_STANDARD"           ,      
    "FORECAST_MANSO_GCAT_STANDARD"           ,      
    "FORECAST_RETAIL_TOT_VOL_KG"           ,      
    "FORECAST_AP_RETAIL_REVENUE_MRRSP"           ,      
    "FORECAST_AP_RETAIL_REVENUE_RSP"           ,      
    "FORECAST_AP_RETAIL_REVENUE_NET"           ,      
    "FORECAST_AP_RETAIL_COST_OF_SALES"           ,      
    "FORECAST_AP_RETAIL_RETAILER_RETRO_FUNDING"           ,      
    "FORECAST_AP_RETAIL_MARGIN_EXCL_FIXED_FUNDING"           ,      
    "FORECAST_AP_RETAIL_PROMO_FIXED_SPEND"           ,      
    "FORECAST_AP_RETAIL_TOTAL_SPEND"           ,      
    "FORECAST_AP_RETAIL_MARGIN_INCL_FIXED_FUNDING"           ,      
    "FORECAST_AP_RETAIL_REVENUE_NET_EXCL_MRRSP"           ,      
    "FORECAST_AP_RETAIL_REVENUE_NET_EXCL_RSP"           ,      
    ROUND("SI_B_VOL_KG",4)           ,      
    ROUND("SI_A_VOL_KG",4)           ,      
    ROUND("SI_T_VOL_KG",4)           ,      
    ROUND("SI_M_VOL_KG",4)           ,      
    ROUND("SI_I_VOL_KG",4)           ,      
    ROUND("SO_B_VOL_KG",4)           ,      
    ROUND("SO_A_VOL_KG",4)           ,      
    ROUND("SO_T_VOL_KG",4)           ,      
    ROUND("SO_M_VOL_KG",4)           ,      
    ROUND("SO_I_VOL_KG",4)           ,      
    ROUND("SI_CANNIB_VOL_KG",4)           ,      
    ROUND("SO_CANNIB_VOL_KG",4)           ,      
    ROUND("SI_CANNIB_BASEVOL_KG",4)           ,      
    ROUND("SO_CANNIB_BASEVOL_KG",4)           ,      
    ROUND("SI_CANNIB_LOSS_VOL_KG",4)           ,      
    ROUND("SO_CANNIB_LOSS_VOL_KG",4)           ,      
    ROUND("SI_PREDIP_VOL_KG",4)           ,      
    ROUND("SI_POSTDIP_VOL_KG",4)           ,      
    ROUND("SO_PREDIP_VOL_KG",4)           ,      
    ROUND("SO_POSTDIP_VOL_KG",4)           ,      
    ROUND("SI_PREDIP_BASEVOL_KG",4)           ,      
    ROUND("SI_POSTDIP_BASEVOL_KG",4)           ,      
    ROUND("SO_PREDIP_BASEVOL_KG",4)           ,      
    ROUND("SO_POSTDIP_BASEVOL_KG",4)           ,      
    ROUND("ACTUALS_TOT_VOL_CA",4)           ,      
    ROUND("ACTUALS_TOT_VOL_UL",4)           ,      
    ROUND("BASE_TOT_VOL_CA",4)           ,      
    ROUND("BASE_TOT_VOL_UL",4)           ,      
    ROUND("FORECAST_TOT_VOL_CA",4)           ,      
    ROUND("FORECAST_TOT_VOL_UL",4)           ,      
    ROUND("V_CA_KG_CONV",4)           ,      
    "PRM_RPT_CUSTOMER_CODE"           ,      
    "PROMO_CODE"           ,      
    "UNIQUE_KEY"      
  

from WBX_DEV.zz_mkundu.conv_wbx_sls_promo_fact


),

b as (

    
select
  
    "SOURCE_SYSTEM"           ,      
    "PROMO_IDX"           ,      
    "CUST_IDX"           ,      
    "PLAN_SOURCE_CUSTOMER_CODE"           ,      
    "CUSTOMER_ADDRESS_NUMBER_GUID"           ,      
    "SKU_IDX"           ,      
    "SOURCE_ITEM_IDENTIFIER"           ,      
    "CALENDAR_DATE"           ,      
    "SNAPSHOT_DATE"           ,      
    "REPORTINGSKU_IDX"           ,      
    "ISPROMOSKU"           ,      
    "ISCANNIBSKU"           ,      
    "ISSI_PREPROMODAY"           ,      
    "ISSI_ONPROMODAY"           ,      
    "ISSI_POSTPROMODAY"           ,      
    "ISSO_PREPROMODAY"           ,      
    "ISSO_ONPROMODAY"           ,      
    "ISSO_POSTPROMODAY"           ,      
    "SI_B_VOL_CSE"           ,      
    "SI_B_VOL_SGL"           ,      
    "SI_A_VOL_CSE"           ,      
    "SI_A_VOL_SGL"           ,      
    "SI_T_VOL_CSE"           ,      
    "SI_T_VOL_SGL"           ,      
    "SI_M_VOL_CSE"           ,      
    "SI_M_VOL_SGL"           ,      
    "SI_I_VOL_CSE"           ,      
    "SI_I_VOL_SGL"           ,      
    "SO_B_VOL_CSE"           ,      
    "SO_B_VOL_SGL"           ,      
    "SO_A_VOL_CSE"           ,      
    "SO_A_VOL_SGL"           ,      
    "SO_T_VOL_CSE"           ,      
    "SO_T_VOL_SGL"           ,      
    "SO_M_VOL_CSE"           ,      
    "SO_M_VOL_SGL"           ,      
    "SO_I_VOL_CSE"           ,      
    "SO_I_VOL_SGL"           ,      
    "SI_CANNIB_VOL_CSE"           ,      
    "SI_CANNIB_VOL_SGL"           ,      
    "SO_CANNIB_VOL_CSE"           ,      
    "SO_CANNIB_VOL_SGL"           ,      
    "SI_CANNIB_BASEVOL_CSE"           ,      
    "SI_CANNIB_BASEVOL_SGL"           ,      
    "SO_CANNIB_BASEVOL_CSE"           ,      
    "SO_CANNIB_BASEVOL_SGL"           ,      
    "SI_CANNIB_LOSS_VOL_CSE"           ,      
    "SI_CANNIB_LOSS_VOL_SGL"           ,      
    "SO_CANNIB_LOSS_VOL_CSE"           ,      
    "SO_CANNIB_LOSS_VOL_SGL"           ,      
    "SI_PREDIP_VOL_CSE"           ,      
    "SI_PREDIP_VOL_SGL"           ,      
    "SI_POSTDIP_VOL_CSE"           ,      
    "SI_POSTDIP_VOL_SGL"           ,      
    "SO_PREDIP_VOL_CSE"           ,      
    "SO_PREDIP_VOL_SGL"           ,      
    "SO_POSTDIP_VOL_CSE"           ,      
    "SO_POSTDIP_VOL_SGL"           ,      
    "SI_PREDIP_BASEVOL_CSE"           ,      
    "SI_PREDIP_BASEVOL_SGL"           ,      
    "SI_POSTDIP_BASEVOL_CSE"           ,      
    "SI_POSTDIP_BASEVOL_SGL"           ,      
    "SO_PREDIP_BASEVOL_CSE"           ,      
    "SO_PREDIP_BASEVOL_SGL"           ,      
    "SO_POSTDIP_BASEVOL_CSE"           ,      
    "SO_POSTDIP_BASEVOL_SGL"           ,      
    "POSTPROMODIPPERCENT_SI"           ,      
    "POSTPROMODIPPERCENT_SO"           ,      
    "PREPROMODIPPERCENT_SI"           ,      
    "PREPROMODIPPERCENT_SO"           ,      
    "ONPROMOPHASINGPERCENT_SI"           ,      
    "ONPROMOPHASINGPERCENT_SO"           ,      
    "ROBFUNDINGREQUIRED"           ,      
    "ACTUALS_TOT_VOL_KG"           ,      
    "ACTUALS_AP_GROSS_SALES_VALUE"           ,      
    "ACTUALS_AP_RANGE_SUPPORT_ALLOWANCE"           ,      
    "ACTUALS_AP_EVERYDAY_LOW_PRICES"           ,      
    "ACTUALS_AP_PERMANENT_DISC"           ,      
    "ACTUALS_AP_OFF_INVOICE_DISC"           ,      
    "ACTUALS_AP_INVOICED_SALES_VALUE"           ,      
    "ACTUALS_AP_EARLY_SETTLEMENT_DISC"           ,      
    "ACTUALS_AP_GROWTH_INCENTIVES"           ,      
    "ACTUALS_AP_NET_SALES_VALUE"           ,      
    "ACTUALS_AP_RETRO"           ,      
    "ACTUALS_AP_AVP_DISC"           ,      
    "ACTUALS_AP_VARIABLE_TRADE"           ,      
    "ACTUALS_AP_PROMO_FIXED_FUNDING"           ,      
    "ACTUALS_AP_RANGE_SUPPORT_INCENTIVES"           ,      
    "ACTUALS_AP_NET_NET_SALES_VALUE"           ,      
    "ACTUALS_AP_DIRECT_SHOPPER_MARKETING"           ,      
    "ACTUALS_AP_OTHER_DIRECT_PAYMENTS"           ,      
    "ACTUALS_AP_INDIRECT_SHOPPER_MARKETING"           ,      
    "ACTUALS_AP_OTHER_INDIRECT_PAYMENTS"           ,      
    "ACTUALS_AP_FIXED_TRADE_CUST_INVOICED"           ,      
    "ACTUALS_AP_TOTAL_TRADE_CUST_INVOICED"           ,      
    "ACTUALS_AP_FIXED_TRADE_NON_CUST_INVOICED"           ,      
    "ACTUALS_AP_TOTAL_TRADE"           ,      
    "ACTUALS_AP_NET_REALISABLE_REVENUE"           ,      
    "ACTUALS_AP_TOT_PRIME_COST_STANDARD"           ,      
    "ACTUALS_AP_GROSS_MARGIN_STANDARD"           ,      
    "ACTUALS_AP_GCAT_STANDARD"           ,      
    "ACTUALS_MANSO_TOT_VOL_KG"           ,      
    "ACTUALS_MANSO_GROSS_SALES_VALUE"           ,      
    "ACTUALS_MANSO_RANGE_SUPPORT_ALLOWANCE"           ,      
    "ACTUALS_MANSO_EVERYDAY_LOW_PRICES"           ,      
    "ACTUALS_MANSO_PERMANENT_DISC"           ,      
    "ACTUALS_MANSO_OFF_INVOICE_DISC"           ,      
    "ACTUALS_MANSO_INVOICED_SALES_VALUE"           ,      
    "ACTUALS_MANSO_EARLY_SETTLEMENT_DISC"           ,      
    "ACTUALS_MANSO_GROWTH_INCENTIVES"           ,      
    "ACTUALS_MANSO_NET_SALES_VALUE"           ,      
    "ACTUALS_MANSO_RETRO"           ,      
    "ACTUALS_MANSO_AVP_DISC"           ,      
    "ACTUALS_MANSO_VARIABLE_TRADE"           ,      
    "ACTUALS_MANSO_PROMO_FIXED_FUNDING"           ,      
    "ACTUALS_MANSO_RANGE_SUPPORT_INCENTIVES"           ,      
    "ACTUALS_MANSO_NET_NET_SALES_VALUE"           ,      
    "ACTUALS_MANSO_DIRECT_SHOPPER_MARKETING"           ,      
    "ACTUALS_MANSO_OTHER_DIRECT_PAYMENTS"           ,      
    "ACTUALS_MANSO_INDIRECT_SHOPPER_MARKETING"           ,      
    "ACTUALS_MANSO_OTHER_INDIRECT_PAYMENTS"           ,      
    "ACTUALS_MANSO_FIXED_TRADE_CUST_INVOICED"           ,      
    "ACTUALS_MANSO_TOTAL_TRADE_CUST_INVOICED"           ,      
    "ACTUALS_MANSO_FIXED_TRADE_NON_CUST_INVOICED"           ,      
    "ACTUALS_MANSO_TOTAL_TRADE"           ,      
    "ACTUALS_MANSO_NET_REALISABLE_REVENUE"           ,      
    "ACTUALS_MANSO_TOT_PRIME_COST_STANDARD"           ,      
    "ACTUALS_MANSO_GROSS_MARGIN_STANDARD"           ,      
    "ACTUALS_MANSO_GCAT_STANDARD"           ,      
    "ACTUALS_RETAIL_TOT_VOL_KG"           ,      
    "ACTUALS_AP_RETAIL_REVENUE_MRRSP"           ,      
    "ACTUALS_AP_RETAIL_REVENUE_RSP"           ,      
    "ACTUALS_AP_RETAIL_REVENUE_NET"           ,      
    "ACTUALS_AP_RETAIL_COST_OF_SALES"           ,      
    "ACTUALS_AP_RETAIL_RETAILER_RETRO_FUNDING"           ,      
    "ACTUALS_AP_RETAIL_MARGIN_EXCL_FIXED_FUNDING"           ,      
    "ACTUALS_AP_RETAIL_PROMO_FIXED_SPEND"           ,      
    "ACTUALS_AP_RETAIL_TOTAL_SPEND"           ,      
    "ACTUALS_AP_RETAIL_MARGIN_INCL_FIXED_FUNDING"           ,      
    "ACTUALS_AP_RETAIL_REVENUE_NET_EXCL_MRRSP"           ,      
    "ACTUALS_AP_RETAIL_REVENUE_NET_EXCL_RSP"           ,      
    "BASE_TOT_VOL_KG"           ,      
    "BASE_AP_GROSS_SALES_VALUE"           ,      
    "BASE_AP_RANGE_SUPPORT_ALLOWANCE"           ,      
    "BASE_AP_EVERYDAY_LOW_PRICES"           ,      
    "BASE_AP_PERMANENT_DISC"           ,      
    "BASE_AP_OFF_INVOICE_DISC"           ,      
    "BASE_AP_INVOICED_SALES_VALUE"           ,      
    "BASE_AP_EARLY_SETTLEMENT_DISC"           ,      
    "BASE_AP_GROWTH_INCENTIVES"           ,      
    "BASE_AP_NET_SALES_VALUE"           ,      
    "BASE_AP_RETRO"           ,      
    "BASE_AP_AVP_DISC"           ,      
    "BASE_AP_VARIABLE_TRADE"           ,      
    "BASE_AP_PROMO_FIXED_FUNDING"           ,      
    "BASE_AP_RANGE_SUPPORT_INCENTIVES"           ,      
    "BASE_AP_NET_NET_SALES_VALUE"           ,      
    "BASE_AP_DIRECT_SHOPPER_MARKETING"           ,      
    "BASE_AP_OTHER_DIRECT_PAYMENTS"           ,      
    "BASE_AP_INDIRECT_SHOPPER_MARKETING"           ,      
    "BASE_AP_OTHER_INDIRECT_PAYMENTS"           ,      
    "BASE_AP_FIXED_TRADE_CUST_INVOICED"           ,      
    "BASE_AP_TOTAL_TRADE_CUST_INVOICED"           ,      
    "BASE_AP_FIXED_TRADE_NON_CUST_INVOICED"           ,      
    "BASE_AP_TOTAL_TRADE"           ,      
    "BASE_AP_NET_REALISABLE_REVENUE"           ,      
    "BASE_AP_TOT_PRIME_COST_STANDARD"           ,      
    "BASE_AP_GROSS_MARGIN_STANDARD"           ,      
    "BASE_AP_GCAT_STANDARD"           ,      
    "BASE_MANSO_TOT_VOL_KG"           ,      
    "BASE_MANSO_GROSS_SALES_VALUE"           ,      
    "BASE_MANSO_RANGE_SUPPORT_ALLOWANCE"           ,      
    "BASE_MANSO_EVERYDAY_LOW_PRICES"           ,      
    "BASE_MANSO_PERMANENT_DISC"           ,      
    "BASE_MANSO_OFF_INVOICE_DISC"           ,      
    "BASE_MANSO_INVOICED_SALES_VALUE"           ,      
    "BASE_MANSO_EARLY_SETTLEMENT_DISC"           ,      
    "BASE_MANSO_GROWTH_INCENTIVES"           ,      
    "BASE_MANSO_NET_SALES_VALUE"           ,      
    "BASE_MANSO_RETRO"           ,      
    "BASE_MANSO_AVP_DISC"           ,      
    "BASE_MANSO_VARIABLE_TRADE"           ,      
    "BASE_MANSO_PROMO_FIXED_FUNDING"           ,      
    "BASE_MANSO_RANGE_SUPPORT_INCENTIVES"           ,      
    "BASE_MANSO_NET_NET_SALES_VALUE"           ,      
    "BASE_MANSO_DIRECT_SHOPPER_MARKETING"           ,      
    "BASE_MANSO_OTHER_DIRECT_PAYMENTS"           ,      
    "BASE_MANSO_INDIRECT_SHOPPER_MARKETING"           ,      
    "BASE_MANSO_OTHER_INDIRECT_PAYMENTS"           ,      
    "BASE_MANSO_FIXED_TRADE_CUST_INVOICED"           ,      
    "BASE_MANSO_TOTAL_TRADE_CUST_INVOICED"           ,      
    "BASE_MANSO_FIXED_TRADE_NON_CUST_INVOICED"           ,      
    "BASE_MANSO_TOTAL_TRADE"           ,      
    "BASE_MANSO_NET_REALISABLE_REVENUE"           ,      
    "BASE_MANSO_TOT_PRIME_COST_STANDARD"           ,      
    "BASE_MANSO_GROSS_MARGIN_STANDARD"           ,      
    "BASE_MANSO_GCAT_STANDARD"           ,      
    "BASE_RETAIL_TOT_VOL_KG"           ,      
    "BASE_AP_RETAIL_REVENUE_MRRSP"           ,      
    "BASE_AP_RETAIL_REVENUE_RSP"           ,      
    "BASE_AP_RETAIL_REVENUE_NET"           ,      
    "BASE_AP_RETAIL_COST_OF_SALES"           ,      
    "BASE_AP_RETAIL_RETAILER_RETRO_FUNDING"           ,      
    "BASE_AP_RETAIL_MARGIN_EXCL_FIXED_FUNDING"           ,      
    "BASE_AP_RETAIL_PROMO_FIXED_SPEND"           ,      
    "BASE_AP_RETAIL_TOTAL_SPEND"           ,      
    "BASE_AP_RETAIL_MARGIN_INCL_FIXED_FUNDING"           ,      
    "BASE_AP_RETAIL_REVENUE_NET_EXCL_MRRSP"           ,      
    "BASE_AP_RETAIL_REVENUE_NET_EXCL_RSP"           ,      
    "FORECAST_TOT_VOL_KG"           ,      
    "FORECAST_AP_GROSS_SALES_VALUE"           ,      
    "FORECAST_AP_RANGE_SUPPORT_ALLOWANCE"           ,      
    "FORECAST_AP_EVERYDAY_LOW_PRICES"           ,      
    "FORECAST_AP_PERMANENT_DISC"           ,      
    "FORECAST_AP_OFF_INVOICE_DISC"           ,      
    "FORECAST_AP_INVOICED_SALES_VALUE"           ,      
    "FORECAST_AP_EARLY_SETTLEMENT_DISC"           ,      
    "FORECAST_AP_GROWTH_INCENTIVES"           ,      
    "FORECAST_AP_NET_SALES_VALUE"           ,      
    "FORECAST_AP_RETRO"           ,      
    "FORECAST_AP_AVP_DISC"           ,      
    "FORECAST_AP_VARIABLE_TRADE"           ,      
    "FORECAST_AP_PROMO_FIXED_FUNDING"           ,      
    "FORECAST_AP_RANGE_SUPPORT_INCENTIVES"           ,      
    "FORECAST_AP_NET_NET_SALES_VALUE"           ,      
    "FORECAST_AP_DIRECT_SHOPPER_MARKETING"           ,      
    "FORECAST_AP_OTHER_DIRECT_PAYMENTS"           ,      
    "FORECAST_AP_INDIRECT_SHOPPER_MARKETING"           ,      
    "FORECAST_AP_OTHER_INDIRECT_PAYMENTS"           ,      
    "FORECAST_AP_FIXED_TRADE_CUST_INVOICED"           ,      
    "FORECAST_AP_TOTAL_TRADE_CUST_INVOICED"           ,      
    "FORECAST_AP_FIXED_TRADE_NON_CUST_INVOICED"           ,      
    "FORECAST_AP_TOTAL_TRADE"           ,      
    "FORECAST_AP_TOTAL_TRADE_GBP"           ,      
    "FORECAST_AP_NET_REALISABLE_REVENUE"           ,      
    "FORECAST_AP_TOT_PRIME_COST_STANDARD"           ,      
    "FORECAST_AP_GROSS_MARGIN_STANDARD"           ,      
    "FORECAST_AP_GCAT_STANDARD"           ,      
    "FORECAST_MANSO_TOT_VOL_KG"           ,      
    "FORECAST_MANSO_GROSS_SALES_VALUE"           ,      
    "FORECAST_MANSO_RANGE_SUPPORT_ALLOWANCE"           ,      
    "FORECAST_MANSO_EVERYDAY_LOW_PRICES"           ,      
    "FORECAST_MANSO_PERMANENT_DISC"           ,      
    "FORECAST_MANSO_OFF_INVOICE_DISC"           ,      
    "FORECAST_MANSO_INVOICED_SALES_VALUE"           ,      
    "FORECAST_MANSO_EARLY_SETTLEMENT_DISC"           ,      
    "FORECAST_MANSO_GROWTH_INCENTIVES"           ,      
    "FORECAST_MANSO_NET_SALES_VALUE"           ,      
    "FORECAST_MANSO_RETRO"           ,      
    "FORECAST_MANSO_AVP_DISC"           ,      
    "FORECAST_MANSO_VARIABLE_TRADE"           ,      
    "FORECAST_MANSO_PROMO_FIXED_FUNDING"           ,      
    "FORECAST_MANSO_RANGE_SUPPORT_INCENTIVES"           ,      
    "FORECAST_MANSO_NET_NET_SALES_VALUE"           ,      
    "FORECAST_MANSO_DIRECT_SHOPPER_MARKETING"           ,      
    "FORECAST_MANSO_OTHER_DIRECT_PAYMENTS"           ,      
    "FORECAST_MANSO_INDIRECT_SHOPPER_MARKETING"           ,      
    "FORECAST_MANSO_OTHER_INDIRECT_PAYMENTS"           ,      
    "FORECAST_MANSO_FIXED_TRADE_CUST_INVOICED"           ,      
    "FORECAST_MANSO_TOTAL_TRADE_CUST_INVOICED"           ,      
    "FORECAST_MANSO_FIXED_TRADE_NON_CUST_INVOICED"           ,      
    "FORECAST_MANSO_TOTAL_TRADE"           ,      
    "FORECAST_MANSO_NET_REALISABLE_REVENUE"           ,      
    "FORECAST_MANSO_TOT_PRIME_COST_STANDARD"           ,      
    "FORECAST_MANSO_GROSS_MARGIN_STANDARD"           ,      
    "FORECAST_MANSO_GCAT_STANDARD"           ,      
    "FORECAST_RETAIL_TOT_VOL_KG"           ,      
    "FORECAST_AP_RETAIL_REVENUE_MRRSP"           ,      
    "FORECAST_AP_RETAIL_REVENUE_RSP"           ,      
    "FORECAST_AP_RETAIL_REVENUE_NET"           ,      
    "FORECAST_AP_RETAIL_COST_OF_SALES"           ,      
    "FORECAST_AP_RETAIL_RETAILER_RETRO_FUNDING"           ,      
    "FORECAST_AP_RETAIL_MARGIN_EXCL_FIXED_FUNDING"           ,      
    "FORECAST_AP_RETAIL_PROMO_FIXED_SPEND"           ,      
    "FORECAST_AP_RETAIL_TOTAL_SPEND"           ,      
    "FORECAST_AP_RETAIL_MARGIN_INCL_FIXED_FUNDING"           ,      
    "FORECAST_AP_RETAIL_REVENUE_NET_EXCL_MRRSP"           ,      
    "FORECAST_AP_RETAIL_REVENUE_NET_EXCL_RSP"           ,      
    ROUND("SI_B_VOL_KG",4)           ,      
    ROUND("SI_A_VOL_KG",4)           ,      
    ROUND("SI_T_VOL_KG",4)           ,      
    ROUND("SI_M_VOL_KG",4)           ,      
    ROUND("SI_I_VOL_KG",4)           ,      
    ROUND("SO_B_VOL_KG",4)           ,      
    ROUND("SO_A_VOL_KG",4)           ,      
    ROUND("SO_T_VOL_KG",4)           ,      
    ROUND("SO_M_VOL_KG",4)           ,      
    ROUND("SO_I_VOL_KG",4)           ,      
    ROUND("SI_CANNIB_VOL_KG",4)          ,      
    ROUND("SO_CANNIB_VOL_KG",4)           ,      
    ROUND("SI_CANNIB_BASEVOL_KG",4)           ,      
    ROUND("SO_CANNIB_BASEVOL_KG",4)           ,      
    ROUND("SI_CANNIB_LOSS_VOL_KG",4)           ,      
    ROUND("SO_CANNIB_LOSS_VOL_KG",4)           ,      
    ROUND("SI_PREDIP_VOL_KG",4)           ,      
    ROUND("SI_POSTDIP_VOL_KG",4)           ,      
    ROUND("SO_PREDIP_VOL_KG",4)           ,      
    ROUND("SO_POSTDIP_VOL_KG",4)           ,      
    ROUND("SI_PREDIP_BASEVOL_KG",4)           ,      
    ROUND("SI_POSTDIP_BASEVOL_KG",4)           ,      
    ROUND("SO_PREDIP_BASEVOL_KG",4)           ,      
    ROUND("SO_POSTDIP_BASEVOL_KG",4)           ,      
    ROUND("ACTUALS_TOT_VOL_CA",4)           ,      
    ROUND("ACTUALS_TOT_VOL_UL",4)           ,      
    ROUND("BASE_TOT_VOL_CA",4)           ,      
    ROUND("BASE_TOT_VOL_UL",4)           ,      
    ROUND("FORECAST_TOT_VOL_CA",4)           ,      
    ROUND("FORECAST_TOT_VOL_UL",4)         ,      
    ROUND("V_CA_KG_CONV",4)           ,      
    "PRM_RPT_CUSTOMER_CODE"           ,      
    "PROMO_CODE"           ,      
    "UNIQUE_KEY"      
  

from WBX_DEV.zz_mkundu.fct_wbx_sls_promo


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
    order by unique_key,  in_a desc, in_b desc

)

select * from final




