{{
        config(
        materialized=env_var("DBT_RZ_DS_MAT"),
        snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
          tags=[
              "redzone",
              "OEE",
              "v_action"
               ]
             )
}}


with source as (

    select * from {{ ref('src_rz_v_action') }}

)

select * from source