

with source as (

    select * from {{ source('WEETABIX', 'dimensionfinancialtag') }}

),

renamed as (

    select
        description,
        value,
        financialtagcategory,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
