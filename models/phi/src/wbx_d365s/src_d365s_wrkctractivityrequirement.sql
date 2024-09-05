with d365_source as (
    select *
    from {{ source("D365S", "wrkctractivityrequirement") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (


    select
        'D365S' as source,
        relationshiptype as relationshiptype,
        activityrequirementset as activityrequirementset,
        usedforjobscheduling as usedforjobscheduling,
        usedforoperationscheduling as usedforoperationscheduling,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source
)

select * from renamed