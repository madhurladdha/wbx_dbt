

with source as (

    select * from {{ source('EI_RDM', 'sls_wtx_budget_scen_xref') }}

),

renamed as (

    select
        source_system,
        scen_id,
        scen_code,
        frozen_forecast,
        current_version_flag,
        sls_budget_snapshot_date,
        delineation_date

    from source

)

select * from renamed
