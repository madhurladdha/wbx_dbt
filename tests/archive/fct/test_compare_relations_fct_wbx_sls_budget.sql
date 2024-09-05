{{
    config(
        enabled=false,
        severity="warn",
        tags=["sales", "budget", "sls_budget", "sls_budget_fin"],
    )
}}


{% set old_etl_relation = ref("conv_sls_wbx_budget_fact") %}

{% set dbt_relation = ref("fct_wbx_sls_budget") %}


{{
    ent_dbt_package.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        exclude_columns=[
            "PLAN_CUSTOMER_ADDR_NUMBER_GUID",
            "SCENARIO_GUID"
        ],
         primary_key=unique_key,
        summarize=true
    )
}}
