{{ config(
  enabled=false,
  severity = 'warn'
) }}

{% set old_etl_relation = ref('conv_fin_onestream_fact') %}

{% set dbt_relation = ref('dim_wbx_fin_onestream') %}



{{ ent_dbt_package.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    exclude_columns=["LOAD_DATE","FILE_NAME"],
    summarize=false
    )
}}
