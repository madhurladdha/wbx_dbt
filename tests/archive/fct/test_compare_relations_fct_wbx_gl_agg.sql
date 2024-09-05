{{ config( 
    enabled=false,
    severity = 'warn', 
    warn_if = '>1' 
) }} 

{% set old_etl_relation=ref('conv_fin_wbx_gl_agg_fact') %}

{% set dbt_relation=ref('fct_wbx_fin_gl_agg') %} 
 

{{audit_helper.compare_relations( a_relation=old_etl_relation, b_relation=dbt_relation,
exclude_columns=[
    "LOAD_DATE",
    "LOAD_STATUS",
    "UPDATE_DATE",
    "ACCOUNT_GUID",
    "GL_ACCT_LINE_GUID",
    "BUSINESS_UNIT_ADDRESS_GUID",
    "TARGET_ACCOUNT_IDENTIFIER"
    "BATCH_NUMBER",
    "SOURCE_DATE_UPDATED"], 
primary_key="UNIQUE_KEY" 
) }} 