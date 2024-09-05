with
d365_source as (
    select *
    from {{ source("D365", "eco_res_product_translation") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (



    select
        'D365' as source,
        description as description,
        name as name,
        product as product,
        language_id as languageid,
        modifiedby as modifiedby,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed
