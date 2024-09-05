with
d365_source as (

    select *
    from {{ source("D365S", "custsettlement") }}
    where
        upper(trim(dataareaid)) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
        and _fivetran_deleted = 'FALSE'
),

renamed as (


    select
        'D365S' as source,
        transrecid as transrecid,
        cast(transdate as TIMESTAMP_NTZ) as transdate,
        offsettransvoucher as offsettransvoucher,
        accountnum as accountnum,
        settleamountcur as settleamountcur,
        settleamountmst as settleamountmst,
        exchadjustment as exchadjustment,
        null as settlementvoucher,
        transcompany as transcompany,
        offsetrecid as offsetrecid,
        cast(duedate as TIMESTAMP_NTZ) as duedate,
        utilizedcashdisc as utilizedcashdisc,
        cast(custcashdiscdate as TIMESTAMP_NTZ) as custcashdiscdate,
        cast(lastinterestdate as TIMESTAMP_NTZ) as lastinterestdate,
        pennydiff as pennydiff,
        canbereversed as canbereversed,
        settletax_1099_amount as settletax1099amount,
        settletax_1099_stateamount as settletax1099stateamount,
        defaultdimension as defaultdimension,
        cashdiscountledgerdimension as cashdiscountledgerdimension,
        null as eusaleslist,
        offsetcompany as offsetcompany,
        offsetaccountnum as offsetaccountnum,
        settlementgroup as settlementgroup,
        settleamountreporting as settleamountreporting,
        exchadjustmentreporting as exchadjustmentreporting,
        interestamount_br as interestamount_br,
        fineamount_br as fineamount_br,
        null as interestcode_br,
        null as finecode_br,
        null as taxvoucher_ru,
        reversedrecid_ru as reversedrecid_ru,
        reversetrans_ru as reversetrans_ru,
        cast(reportingdate_ru as TIMESTAMP_NTZ) as reportingdate_ru,
        cast(createddatetime as TIMESTAMP_NTZ) as createddatetime,
        null as del_createdtime,
        createdby as createdby,
        upper(trim(dataareaid)) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source
)

select * from renamed
