with source as (

    select * from {{ source('WEETABIX', 'vendsettlement') }}

),

renamed as (

    select
        transrecid,
        transdate,
        offsettransvoucher,
        accountnum,
        settleamountcur,
        settleamountmst,
        exchadjustment,
        settlementvoucher,
        vendpaymentgroup,
        statusvoucher,
        offsetrecid,
        duedate,
        utilizedcashdisc,
        vendcashdiscdate,
        lastinterestdatedummy,
        pennydiff,
        canbereversed,
        transcompany,
        settletax1099amount,
        settletax1099stateamount,
        cashdiscountledgerdimension,
        defaultdimension,
        offsetcompany,
        offsetaccountnum,
        settlementgroup,
        remittanceaddress,
        eusaleslist,
        settleamountreporting,
        exchadjustmentreporting,
        interestamount_br,
        fineamount_br,
        interestcode_br,
        finecode_br,
        thirdpartybankaccountid,
        taxvoucher_ru,
        reversedrecid_ru,
        reversetrans_ru,
        vattaxagentamount_ru,
        reportingdate_ru,
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
