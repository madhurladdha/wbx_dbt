with source as (

    select * from {{ source('WEETABIX', 'wrkctractivityrequirement') }}

),

renamed as (

    select
        relationshiptype,
        activityrequirementset,
        usedforjobscheduling,
        usedforoperationscheduling,
        recversion,
        partition,
        recid

    from source

)

select * from renamed