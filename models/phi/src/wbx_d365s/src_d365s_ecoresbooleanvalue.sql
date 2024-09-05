

with source as (

    select *
    from {{ source('D365S', 'ecoresbooleanvalue') }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        recid,
        _fivetran_synced,
        null as ssysrowid,
        null as datalakemodifieddatetime,
        booleanvalue as boolean_value,
        _fivetran_deleted

    from source

)

select * from renamed

