{{
    config(
        materialized=env_var("DBT_MAT_TABLE"),
        tags=["manufacturing", "cbom","mfg_cbom","sales", "terms","sls_terms"],
    )
}}
with bomcalctrans as (
    select * from {{ ref('src_bomcalctrans')}}
)
select * from bomcalctrans