with
d365_source as (
    select *
    from {{ source("D365S", "dimensionfinancialtag") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (


    select
        'D365S' as source,
        description,
        value,
        financialtagcategory as financialtagcategory,
        recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed

