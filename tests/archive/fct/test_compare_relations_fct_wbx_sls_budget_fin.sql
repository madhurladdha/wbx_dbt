{{
    config(
        enabled=false,
        severity="warn",
        tags=["sales", "budget", "sls_budget", "sls_budget_fin"],
    )
}}


{% set old_etl_relation = ref("conv_sls_wbx_fin_budget_fact") %}

{% set dbt_relation = ref("fct_wbx_sls_budget_fin") %}


{{
    ent_dbt_package.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        exclude_columns=[
            "item_guid",
            "ship_customer_address_guid",
            "bill_customer_address_guid",
            "load_date",
            "update_date",
            "UNIQUE_KEY"
        ],
        summarize=false,
    )
}}
