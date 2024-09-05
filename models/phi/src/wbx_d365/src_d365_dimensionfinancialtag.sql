with
d365_source as (
    select *
    from {{ source("D365", "dimension_financial_tag") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (


    select
        'D365' as source,
        description,
        value,
        financial_tag_category as financialtagcategory,
        recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed

