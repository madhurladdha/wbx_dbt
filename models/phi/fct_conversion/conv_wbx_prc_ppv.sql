{{
    config(
        materialized='view'
    )
}}

with history as (

    select * from {{ source('FACTS_FOR_COMPARE','prc_wtx_ppv_fact') }}

)
select
    *
from history