with d365_source as (
    select *
    from {{ source("D365", "wrk_ctr_activity_requirement_set") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (


    select
        'D365' as source,
        activity as activity,
        quantity as quantity,
        load_percent as loadpercent,
        null as description,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed