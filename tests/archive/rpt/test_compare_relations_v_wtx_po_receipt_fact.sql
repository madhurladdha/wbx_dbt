{{ config(
            enabled=false,
            snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
            severity = 'warn',
            warn_if = '>0' ,
            tags=["finance", "po"]
) }}

{% set old_etl_relation=source('FACTS_FOR_COMPARE','v_wtx_po_receipt_fact') %}
{% set dbt_relation=ref('v_wtx_po_receipt_fact') %}

{{ ent_dbt_package.compare_relations(
a_relation=old_etl_relation,
b_relation=dbt_relation,
exclude_columns=[
"source_date_updated",
"load_date",
"update_date",
],
summarize=false
) }}
