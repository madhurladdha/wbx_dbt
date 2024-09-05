with source as (

    select * from {{ source('WEETABIX', 'custsettlement') }}

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
        transcompany,
        offsetrecid,
        duedate,
        utilizedcashdisc,
        custcashdiscdate,
        lastinterestdate,
        pennydiff,
        canbereversed,
        settletax1099amount,
        settletax1099stateamount,
        defaultdimension,
        cashdiscountledgerdimension,
        eusaleslist,
        offsetcompany,
        offsetaccountnum,
        settlementgroup,
        settleamountreporting,
        exchadjustmentreporting,
        interestamount_br,
        fineamount_br,
        interestcode_br,
        finecode_br,
        taxvoucher_ru,
        reversedrecid_ru,
        reversetrans_ru,
        reportingdate_ru,
        createddatetime,
        del_createdtime,
        createdby,
        upper(trim(dataareaid)) as dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
