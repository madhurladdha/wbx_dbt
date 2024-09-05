{{ config(
  enabled=false,
  severity = 'warn'
) }}


{% set old_etl_relation = source("FACTS_FOR_COMPARE","sls_wtx_promo_dim_hist") %}

{% set dbt_relation=ref('dim_wbx_promo_hist') %}


{{ ent_dbt_package.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    exclude_columns=["promo_guid"],
    summarize=false
    )
}}