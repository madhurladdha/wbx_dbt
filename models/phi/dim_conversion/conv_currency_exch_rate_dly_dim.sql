{{
    config(
    materialized = env_var('DBT_MAT_VIEW'),
    )
}}

with history as (
select * from {{ source('EI_RDM','currency_exch_rate_dly_dim_oc') }} WHERE SOURCE_SYSTEM ='{{env_var("DBT_SOURCE_SYSTEM")}}' and {{env_var("DBT_PICK_FROM_CONV")}}='Y'

)


select * from history