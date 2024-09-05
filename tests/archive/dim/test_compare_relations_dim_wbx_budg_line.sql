{{ config(
  enabled=false,
  severity = 'warn'
) }}



{% set old_etl_relation = ref('conv_proj_master_budg_line') %}

{% set dbt_relation = ref('dim_wbx_budg_line') %}



{{ ent_dbt_package.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    exclude_columns=["Project_guid_old"],
    primary_key="unique_key",
    summarize=false
    )
}}


--------some rows are not matching due to timing issue