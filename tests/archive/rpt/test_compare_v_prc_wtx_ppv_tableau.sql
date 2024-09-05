{{ config(enabled=false, severity="warn", warn_if=">0", tags=["procurement", "ppv"]) }}


{% set old_etl_relation = source("FACTS_FOR_COMPARE", "v_prc_wtx_ppv_tableau") %}

{% set dbt_relation = ref("v_prc_wtx_ppv_tableau") %}

{{
    ent_dbt_package.compare_relations(
        a_relation=old_etl_relation, b_relation=dbt_relation, summarize=false
    )
}}
