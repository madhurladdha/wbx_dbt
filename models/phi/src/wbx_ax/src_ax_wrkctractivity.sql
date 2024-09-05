with source as (

    select * from {{ source('WEETABIX', 'wrkctractivity') }}

),

renamed as (

    select
        entitytype,
        recversion,
        partition,
        recid

    from source

)

select * from renamed