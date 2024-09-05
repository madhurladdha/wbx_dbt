with d365_source as (
    select *
    from {{ source("D365S", "projbudgetline") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (


    select
        'D365S' as source,
        projallocationmethod as projallocationmethod,
        categoryid as categoryid,
        projtranstype as projtranstype,
        originalbudget as originalbudget,
        committedrevisions as committedrevisions,
        uncommittedrevisions as uncommittedrevisions,
        totalbudget as totalbudget,
        projbudgetlinetype as projbudgetlinetype,
        projbudget as projbudget,
        null as activitynumber,
        projid as projid,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        modifiedby as modifiedby,
        cast(createddatetime as TIMESTAMP_NTZ) as createddatetime,
        createdby as createdby,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source
    where upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}

)

select * from renamed