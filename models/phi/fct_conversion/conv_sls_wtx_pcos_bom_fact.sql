{{
    config(
        materialized='view'
    )
}}

with history as (

    select * from {{ source('FACTS_FOR_COMPARE','sls_wtx_pcos_bom_fact') }}

)
select
    *
from history