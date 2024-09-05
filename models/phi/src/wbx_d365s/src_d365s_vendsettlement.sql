with d365_source as (
    select *
    from {{ source("D365S", "vendsettlement") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}

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
        settlementvoucher as settlementvoucher,
        null as vendpaymentgroup,
        null as statusvoucher,
        offsetrecid as offsetrecid,
        cast(duedate as TIMESTAMP_NTZ) as duedate,
        utilizedcashdisc as utilizedcashdisc,
        cast(vendcashdiscdate as TIMESTAMP_NTZ) as vendcashdiscdate,
        cast(lastinterestdatedummy as TIMESTAMP_NTZ) as lastinterestdatedummy,
        pennydiff as pennydiff,
        canbereversed as canbereversed,
        transcompany as transcompany,
        settletax_1099_amount as settletax1099amount,
        settletax_1099_stateamount as settletax1099stateamount,
        cashdiscountledgerdimension as cashdiscountledgerdimension,
        defaultdimension as defaultdimension,
        offsetcompany as offsetcompany,
        offsetaccountnum as offsetaccountnum,
        settlementgroup as settlementgroup,
        remittanceaddress as remittanceaddress,
        null as eusaleslist,
        settleamountreporting as settleamountreporting,
        exchadjustmentreporting as exchadjustmentreporting,
        interestamount_br as interestamount_br,
        fineamount_br as fineamount_br,
        null as interestcode_br,
        null as finecode_br,
        null as thirdpartybankaccountid,
        null as taxvoucher_ru,
        reversedrecid_ru as reversedrecid_ru,
        reversetrans_ru as reversetrans_ru,
        vattaxagentamount_ru as vattaxagentamount_ru,
        cast(reportingdate_ru as TIMESTAMP_NTZ) as reportingdate_ru,
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
