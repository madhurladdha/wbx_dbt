with d365_source as (
    select *
    from {{ source("D365S", "wrkctractivityrequirementset") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (


    select
        'D365S' as source,
        activity as activity,
        quantity as quantity,
        loadpercent as loadpercent,
        null as description,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed