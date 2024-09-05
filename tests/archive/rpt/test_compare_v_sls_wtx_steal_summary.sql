{{ config(enabled=false, severity="warn", warn_if=">0") }}


{% set old_etl_relation = source("FACTS_FOR_COMPARE", "v_sls_wtx_steal_summary") %}

{% set dbt_relation = ref("v_sls_wtx_steal_summary") %}

{{
    ent_dbt_package.compare_relations(
        a_relation=old_etl_relation, b_relation=dbt_relation, summarize=false
    )
}}
