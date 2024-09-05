with d365_source as (
    select *
    from {{ source("D365S", "reqplanversion") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(reqplandataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (

    select
        'D365S' as source,
        active as active,
        upper(reqplandataareaid) as reqplandataareaid,
        reqplanid as reqplanid,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source
)

select * from renamed

