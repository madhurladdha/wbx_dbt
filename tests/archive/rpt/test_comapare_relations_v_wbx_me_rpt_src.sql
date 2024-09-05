{{ config(
            enabled=false,
            severity = 'warn',
            warn_if = '>0' ,
            
) }}

{% set old_etl_relation=source('PHI_ML','v_wbx_me_rpt_src') %}
{% set dbt_relation=ref('v_wbx_me_rpt_src') %}

{{ ent_dbt_package.compare_relations(
a_relation=old_etl_relation,
b_relation=dbt_relation,
exclude_columns=[],
summarize=false
) }}
