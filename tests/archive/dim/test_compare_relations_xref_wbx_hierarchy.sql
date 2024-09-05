
{{ config(
  enabled=false,
  severity = 'warn'
) }}

{% set old_etl_relation = ref('conv_ref_hierarchy_xref') %}

{% set dbt_relation = ref('xref_wbx_hierarchy') %}


{{ ent_dbt_package.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    exclude_columns=["UPDATE_DATE","LOAD_DATE"],
    primary_key='NODE_6',
    summarize=false
    )
}}
