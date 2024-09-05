{{
    config(
        materialized='view'
    )
}}

with history as (

    select * from {{ source('EI_RDM','prc_wtx_forecast_exch_rate_dim') }} where {{env_var("DBT_PICK_FROM_CONV")}}='Y' /*adding variable to include/exclude conversion model data.if variable DBT_PICK_FROM_CONV has value 'Y' then conversion model will pull data from hist else it will be null */

)
select
    *
from history