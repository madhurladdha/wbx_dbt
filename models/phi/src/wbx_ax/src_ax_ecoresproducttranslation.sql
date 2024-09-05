

with source as (

    select * from {{ source('WEETABIX', 'ecoresproducttranslation') }}

),

renamed as (

    select
        description,
        name,
        product,
        languageid,
        modifiedby,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
