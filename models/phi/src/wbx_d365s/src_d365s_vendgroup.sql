with d365_source as (
    select *
    from {{ source("D365S", "vendgroup") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}

),

renamed as (

    select
        'D365S' as source,
        vendgroup as vendgroup,
        name as name,
        null as clearingperiod,
        null as paymtermid,
        null as taxgroupid,
        null as taxperiodpaymentcode_pl,
        excludefromsignup_psn as excludefromsignup_psn,
        cast(createddatetime as TIMESTAMP_NTZ) as createddatetime,
        null as del_createdtime,
        createdby as createdby,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed
