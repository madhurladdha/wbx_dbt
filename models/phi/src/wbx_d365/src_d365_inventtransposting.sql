with 
    d365_source as (
        select *
        from {{ source("D365", "invent_trans_posting") }} where _FIVETRAN_DELETED='FALSE' and upper(data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}
    ),

    renamed as (


        select
            'D365' as source,
            item_id as itemid,
            trans_date as transdate,
            voucher as voucher,
            posting_type as postingtype,
            posting_type_offset as postingtypeoffset,
            invent_trans_posting_type as inventtranspostingtype,
            is_posted as isposted,
            null as projid,
            invent_trans_origin as inventtransorigin,
            ledger_dimension as ledgerdimension,
            offset_ledger_dimension as offsetledgerdimension,
            default_dimension as defaultdimension,
            trans_begin_time as transbegintime,
            transbegintimetzid as transbegintimetzid,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid

        from d365_source

    )

select * from renamed 


