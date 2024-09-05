with d365_source as (
    select *
    from {{ source("D365S", "reasontableref") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (
    select
        'D365S' as source,
        reason as reason,
        reasoncomment as reasoncomment,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed
