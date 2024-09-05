with d365_source as (
    select *
    from {{ source("D365", "main_account_category") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365' as source,
        account_category as accountcategory,
        description as description,
        account_type as accounttype,
        closed as closed,
        account_category_ref as accountcategoryref,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source


)

select * from renamed