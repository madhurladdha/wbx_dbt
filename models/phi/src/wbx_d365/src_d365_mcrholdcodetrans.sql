with d365_source as (
        select *
        from {{ source("D365", "mcrhold_code_trans") }} where _FIVETRAN_DELETED='FALSE'
    ),

    renamed as (

        select
            'D365' as source,
            mcrhold_code as mcrholdcode,
            mcrcleared as mcrcleared,
            null as mcruser,
            null as mcrcomment,
            null as mcrreasoncode,
            invent_ref_id as inventrefid,
            null as mcrholdcodecomment,
            mcrcleared_user as mcrcleareduser,
            mcrhold_user as mcrholduser,
            mcrchecked_out as mcrcheckedout,
            null as mcrcheckedoutto,
            mcrchecked_out_date_time as mcrcheckedoutdatetime,
            mcrcheckedoutdatetimetzid as mcrcheckedoutdatetimetzid,
            mcrhold_clear_date_time as mcrholdcleardatetime,
            mcrholdcleardatetimetzid as mcrholdcleardatetimetzid,
            mcrhold_date_time as mcrholddatetime,
            mcrholddatetimetzid as mcrholddatetimetzid,
            null as retailinfocodeid,
            modifieddatetime as modifieddatetime,
            null as del_modifiedtime,
            modifiedby as modifiedby,
            modifiedtransactionid as modifiedtransactionid,
            createddatetime as createddatetime,
            null as del_createdtime,
            createdby as createdby,
            createdtransactionid as createdtransactionid,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid
        from d365_source   where upper(dataareaid) in {{env_var("DBT_D365_COMPANY_FILTER")}}

    )

select * from renamed