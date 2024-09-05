

with source as (

    select * from {{ source('WEETABIX', 'dimensionfocusledgerdimref') }}

),

renamed as (

    select
        focusledgerdimension,
        accountentryledgerdimension,
        focusdimensionhierarchy,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
