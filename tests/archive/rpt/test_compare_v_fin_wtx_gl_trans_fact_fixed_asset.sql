{{
    config(
        enabled=false,
        severity="warn",
        warn_if=">0",
        tags=["finance", "gl", "trans", "fixed_asset"],
    )
}}


{% set old_etl_relation = source(
    "FACTS_FOR_COMPARE", "v_fin_wtx_gl_trans_fact_fixed_asset"
) %}

{% set dbt_relation = ref("v_fin_wtx_gl_trans_fact_fixed_asset") %}

{{
    ent_dbt_package.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        exclude_columns=["load_date", "update_date"],
        summarize=false,
    )
}}
