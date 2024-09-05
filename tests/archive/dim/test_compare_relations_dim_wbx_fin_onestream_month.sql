{{ config(
  enabled=false,
  severity = 'warn'
) }}

{% set old_etl_relation = ref('conv_fin_onestream_month_fact') %}

{% set dbt_relation = ref('dim_wbx_fin_onestream_month') %}



{{ ent_dbt_package.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    exclude_columns=["LOAD_DATE","UPDATE_DATE"],
    primary_key="UNIQUE_KEY",
    summarize=false
    )
}}
