with d365_source as (
        select *
        from {{ source("D365S", "mcrholdcodetrans") }} where _FIVETRAN_DELETED='FALSE'
    ),

    renamed as (

        select
            'D365S' as source,
            mcrholdcode as mcrholdcode,
            mcrcleared as mcrcleared,
            null as mcruser,
            null as mcrcomment,
            null as mcrreasoncode,
            inventrefid as inventrefid,
            null as mcrholdcodecomment,
            mcrcleareduser as mcrcleareduser,
            mcrholduser as mcrholduser,
            mcrcheckedout as mcrcheckedout,
            null as mcrcheckedoutto,
            cast(mcrcheckedoutdatetime as TIMESTAMP_NTZ) as mcrcheckedoutdatetime,
            null as mcrcheckedoutdatetimetzid,
            cast(mcrholdcleardatetime as TIMESTAMP_NTZ) as mcrholdcleardatetime,
            null as mcrholdcleardatetimetzid,
            cast(mcrholddatetime as TIMESTAMP_NTZ) as mcrholddatetime,
            null as mcrholddatetimetzid,
            null as retailinfocodeid,
            cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
            null as del_modifiedtime,
            modifiedby as modifiedby,
            modifiedtransactionid as modifiedtransactionid,
            cast(createddatetime as TIMESTAMP_NTZ) as createddatetime,
            null as del_createdtime,
            createdby as createdby,
            createdtransactionid as createdtransactionid,
            upper(dataareaid) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid
        from d365_source   where upper(dataareaid) in {{env_var("DBT_D365_COMPANY_FILTER")}}

    )

select * from renamed