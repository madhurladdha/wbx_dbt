{{ config(
  enabled=false,
  severity = 'warn'
) }}


{% set old_etl_relation = ref("conv_sls_wtx_promo_hist") %}

{% set dbt_relation=ref('fct_wbx_sls_promo_hist') %}


{{ ent_dbt_package.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    exclude_columns=["promo_guid"],
    primary_key=unique_key,
    summarize=false
    )
}}