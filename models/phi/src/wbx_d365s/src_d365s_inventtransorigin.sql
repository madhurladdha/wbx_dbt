with
d365_source as (
    select *
    from {{ source("D365S", "inventtransorigin") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (


    select
        'D365S' as source,
        inventtransid as inventtransid,
        referencecategory as referencecategory,
        referenceid as referenceid,
        itemid as itemid,
        iteminventdimid as iteminventdimid,
        party as party,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid,
        null as modifieddatetime,
        null as modifiedby,
        null as createddatetime,
        null as createdby

    from d365_source

)

select * from renamed