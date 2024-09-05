{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation = ref('fct_wbx_prc_itmcst_month_dim') %} 

{% set dbt_relation= 'wbx_prod.fact.fct_wbx_prc_itmcst_month_dim'  %} 

{% set filter_field= "1" %} 

{% set filter_values= "'1'"  %} 



{{ compare_relations_d365( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,

   exclude_columns=[
            "item_guid",
            "business_unit_address_guid",
            "account_guid",
            "load_date",
            "update_date"
        ], 

    primary_key="UNIQUE_KEY",
    summarize=false 

) }} 