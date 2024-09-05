

with source as (

    select * from {{ source('WEETABIX', 'mainaccountcategory') }}

),

renamed as (

    select
        accountcategory,
        description,
        accounttype,
        closed,
        accountcategoryref,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
