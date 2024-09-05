

with source as (

    select * from {{ source('WEETABIX', 'vendgroup') }}

),

renamed as (

    select
        vendgroup,
        name,
        clearingperiod,
        paymtermid,
        taxgroupid,
        taxperiodpaymentcode_pl,
        excludefromsignup_psn,
        createddatetime,
        del_createdtime,
        createdby,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
