{{
    config(
    materialized = 'table',
    transient = false,
    tags = "rdm_core"
    )
}}

with source as (
    select * from {{ref('conv_adr_business_rep_dim') }}
),

renamed as (

    select
        generic_address_type,
        rep_address_number_guid,
        rep_address_number_guid_old,
        source_system,
        source_system_address_number,
        representative_type,
        representative_name,
        date_inserted,
        date_updated,
        program_id

    from source

)

select * from renamed