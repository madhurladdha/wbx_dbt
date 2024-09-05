with d365_source as (
    select *
    from {{ source("D365", "wrk_ctr_activity_requirement") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (


    select
        'D365' as source,
        relationship_type as relationshiptype,
        activity_requirement_set as activityrequirementset,
        used_for_job_scheduling as usedforjobscheduling,
        used_for_operation_scheduling as usedforoperationscheduling,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source
)

select * from renamed