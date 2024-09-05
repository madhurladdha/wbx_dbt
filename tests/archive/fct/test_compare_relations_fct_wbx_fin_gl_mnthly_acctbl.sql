{{ config( 
    enabled=false,
    severity = 'warn', 
    warn_if = '>0' 
) }} 

 

{% set old_etl_relation=ref('conv_fin_wbx_gl_mnthly_acctbal') %} 

 

{% set dbt_relation=ref('fct_wbx_fin_gl_mnthly_acctbal') %} 

 

{{
    ent_dbt_package.compare_relations( a_relation=old_etl_relation, b_relation=dbt_relation,
    exclude_columns=[
    "LOAD_DATE",
    "LOAD_STATUS",
    "UPDATE_DATE",
    "ACCOUNT_GUID",
    "ADDRESS_GUID",
    "BUSINESS_UNIT_ADDRESS_GUID"
     ], 
primary_key="UNIQUE_KEY",
summarize=true 
) 
}} 