with d365_source as (
    select *
    from {{ source("D365S", "mainaccountcategory") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365S' as source,
        accountcategory as accountcategory,
        description as description,
        accounttype as accounttype,
        closed as closed,
        accountcategoryref as accountcategoryref,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source


)

select * from renamed