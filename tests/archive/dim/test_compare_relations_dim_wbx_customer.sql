{{ config(
  enabled=false,
  severity = 'warn'
) }}



{% set old_etl_relation = ref('conv_adr_customer_master_dim') %}

{% set dbt_relation = ref('dim_wbx_customer') %}



{{ ent_dbt_package.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    exclude_columns=["DATE_INSERTED","DATE_UPDATED"],
    primary_key="UNIQUE_KEY",
    summarize=false
    )
}}
