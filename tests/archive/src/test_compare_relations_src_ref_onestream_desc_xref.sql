{{ config(
  enabled=false,
  severity = 'warn',
  warn_if = '>0'
) }}

{% set old_etl_relation=source('EI_RDM','ref_onestream_desc_xref') %}

{% set dbt_relation=ref('src_ref_onestream_desc_xref') %}
{{ ent_dbt_package.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    exclude_columns=["AGGREGATIONWEIGHT"],
    summarize=false
) }}
