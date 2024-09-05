with source as (

    select *
    from {{ source('D365S', 'ecoresproductinstancevalue') }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        'D365S' as source,
        recid,
        product
    from source

)

select * from renamed