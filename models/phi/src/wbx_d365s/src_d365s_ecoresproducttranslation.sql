with
d365_source as (
    select *
    from {{ source("D365S", "ecoresproducttranslation") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (



    select
        'D365S' as source,
        description as description,
        name as name,
        product as product,
        languageid as languageid,
        modifiedby as modifiedby,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed
