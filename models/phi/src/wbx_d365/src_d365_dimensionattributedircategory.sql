with
d365_source as (
    select *
    from {{ source("D365", "dimension_attribute_dir_category") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (


    select
        'D365' as source,
        dimension_attribute as dimensionattribute,
        dir_category as dircategory,
        recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed
