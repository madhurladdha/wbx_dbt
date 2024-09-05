with d365_source as (
    select *
    from {{ source("D365S", "wrkctrprodrouteactivity") }}
    where
        _fivetran_deleted = 'FALSE'
        and trim(
            upper(routedataareaid)
        ) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (


    select
        'D365' as source,
        activity as activity,
        upper(routedataareaid) as routedataareaid,
        prodid as prodid,
        oprnum as oprnum,
        oprpriority as oprpriority,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed