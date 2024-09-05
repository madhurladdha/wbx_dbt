

with source as (

    select * from {{ source('EI_RDM', 'sls_scenario_dim') }}

),

renamed as (

    select
        source_system,
        scenario_guid,
        scenario_id,
        scenario_code,
        scenario_desc,
        scenario_type_id,
        scenario_type_code,
        scenario_type_name,
        scenario_status_id,
        scenario_status_code,
        scenario_status_desc,
        scenario_status_colour,
        scenario_status_description,
        load_date,
        update_date

    from source

)

select * from renamed
