

with source as (

    select * from {{ source('WEETABIX', 'mcrholdcodetrans') }}

),

renamed as (

    select
        mcrholdcode,
        mcrcleared,
        mcruser,
        mcrcomment,
        mcrreasoncode,
        inventrefid,
        mcrholdcodecomment,
        mcrcleareduser,
        mcrholduser,
        mcrcheckedout,
        mcrcheckedoutto,
        mcrcheckedoutdatetime,
        mcrcheckedoutdatetimetzid,
        mcrholdcleardatetime,
        mcrholdcleardatetimetzid,
        mcrholddatetime,
        mcrholddatetimetzid,
        retailinfocodeid,
        modifieddatetime,
        del_modifiedtime,
        modifiedby,
        modifiedtransactionid,
        createddatetime,
        del_createdtime,
        createdby,
        createdtransactionid,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
