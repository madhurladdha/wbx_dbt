
{{ config(
  enabled=false,
  severity = 'warn'
) }}

{% set a_relation=ref('conv_adr_wtx_work_center') %}

{% set b_relation=ref('dim_wbx_work_center') %}

{{ ent_dbt_package.compare_relations(
    a_relation=a_relation,
    b_relation=b_relation,
    exclude_columns=["UPDATE_DATE"],
    summarize=false
) }}
