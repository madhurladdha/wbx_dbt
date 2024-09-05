

with source as (

    select * from {{ source('WEETABIX', 'projbudgetline') }}

),

renamed as (

    select
        projallocationmethod,
        categoryid,
        projtranstype,
        originalbudget,
        committedrevisions,
        uncommittedrevisions,
        totalbudget,
        projbudgetlinetype,
        projbudget,
        activitynumber,
        projid,
        modifieddatetime,
        modifiedby,
        createddatetime,
        createdby,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
