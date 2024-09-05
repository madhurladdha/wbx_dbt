{{ config( 
    enabled=false,
    severity = 'warn', 
    warn_if = '>0' 
) }} 

 

{% set old_etl_relation=ref('conv_fin_wbx_gl_trans_fact') %} 

 

{% set dbt_relation=ref('fct_wbx_fin_gl_trans') %} 

 

{{
    ent_dbt_package.compare_relations( a_relation=old_etl_relation, b_relation=dbt_relation,
    exclude_columns=[
    "LOAD_DATE",
    "LOAD_STATUS",
    "UPDATE_DATE",
    "CUSTOMER_ADDRESS_NUMBER_GUID",
    "ACCOUNT_GUID",
    "ADDRESS_GUID",
    "CUST_PRNT_ADDRESS_NUMBER_GUID",
    "BUSINESS_UNIT_ADDRESS_GUID",
    "SOURCE_BUSINESS_UNIT_CODE",
    "SUBLEDGER_GUID",
    "PAYMENT_TERMS_GUID",
    "GL_ACCT_LINE_GUID",
    "ITEM_GUID",
    "TARGET_ACCOUNT_IDENTIFIER",
    "BATCH_NUMBER",
    "SOURCE_DATE_UPDATED"], 
primary_key="UNIQUE_KEY",
summarize=true 
) 
}} 
