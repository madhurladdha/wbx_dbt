
with d365_source as (
    select *
    from {{ source("D365S", "wrkctractivityresourcerequirement") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(
            trim(resourcedataareaid)
        ) in {{ env_var("DBT_D365_COMPANY_FILTER") }}

),

renamed as (


    select
        'D365S' as source,
        activityrequirement as activityrequirement,
        upper(resourcedataareaid) as resourcedataareaid,
        wrkctrid as wrkctrid,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed