{{ config(
            enabled=false,
            severity = 'warn',
            warn_if = '>0' ,
            sql_header="set (months_of_hist,key_mix_hist,single_day_bool,exclude_covid) = (6,6,1,0);"
) }}


{% set old_etl_relation = source("PHI_ML", "v_ml_wbx_me_input") %}

{% set dbt_relation = ref("v_ml_wbx_me_input") %}

{{
    ent_dbt_package.compare_relations
    (
        a_relation=old_etl_relation, 
        b_relation=dbt_relation, 
        summarize=false
    )
}}



