with
d365_source as (
    select *
    from {{ source("D365S", "inventitempurchsetup") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(trim(dataareaid)) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (


    select
        'D365S' as source,
        itemid as itemid,
        inventdimid as inventdimid,
        inventdimiddefault as inventdimiddefault,
        mandatoryinventsite as mandatoryinventsite,
        mandatoryinventlocation as mandatoryinventlocation,
        multipleqty as multipleqty,
        lowestqty as lowestqty,
        highestqty as highestqty,
        standardqty as standardqty,
        leadtime as leadtime,
        calendardays as calendardays,
        stopped as stopped,
        override as override,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed