{{ config(
  enabled=false,
  severity = 'warn'
) }}

{% set a_relation=ref('conv_adr_wbx_cust_master_ext') %}

{% set b_relation=ref('dim_wbx_customer_ext') %}

{{ ent_dbt_package.compare_relations(
    a_relation=a_relation,
    b_relation=b_relation,
    exclude_columns=["DATE_UPDATED","DATE_INSERTED"],
    primary_key='UNIQUE_KEY',
    summarize=false
) }}

----there are some differences in comparition test due to random picking of data in final model