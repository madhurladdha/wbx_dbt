{{
    config(
        enabled=false,
        severity="warn",
        tags=["wbx","manufacturing","demand","agg",],
    )
}}


{% set old_etl_relation = ref("conv_fct_wbx_mfg_demand_comp_agg") %}

{% set dbt_relation = ref("fct_wbx_mfg_demand_comp_agg") %}


{{
    ent_dbt_package.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        exclude_columns=[
            "load_date",
            "update_date"
        ],
        summarize=false,
    )
}}
