with
    d365_source as (
        select *
        from {{ source("D365", "invent_journal_table") }} where _FIVETRAN_DELETED='FALSE' and upper(trim(data_area_id)) in {{env_var("DBT_D365_COMPANY_FILTER")}} 
    ),

    renamed as (

       

        select
            'D365' as source,
            journal_id as journalid,
            description as description,
            posted as posted,
            reservation as reservation,
            system_blocked as systemblocked,
            null as blockuserid,
            session_login_date_time as sessionlogindatetime,
            sessionlogindatetimetzid as sessionlogindatetimetzid,
            posted_date_time as posteddatetime,
            posteddatetimetzid as posteddatetimetzid,
            journal_type as journaltype,
            journal_name_id as journalnameid,
            invent_dim_fixed as inventdimfixed,
            null as blockusergroupid,
            voucher_draw as voucherdraw,
            voucher_change as voucherchange,
            session_id as sessionid,
            posted_user_id as posteduserid,
            num_of_lines as numoflines,
            journal_id_orignal as journalidorignal,
            detail_summary as detailsummary,
            delete_posted_lines as deletepostedlines,
            ledger_dimension as ledgerdimension,
            worker as worker,
            voucher_number_sequence_table as vouchernumbersequencetable,
            storno_ru as storno_ru,
            null as offsessionid_ru,
            retail_replenishment_type as retailreplenishmenttype,
            null as fshreplenishmentref,
            retail_retail_status_type as retailretailstatustype,
            invent_doc_type_pl as inventdoctype_pl,
            invent_location_id as inventlocationid,
            invent_site_id as inventsiteid,
            null as source_data,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid,
            null as modifieddatetime,
            null as modifiedby,
            null as createddatetime,
            null as createdby

        from d365_source

    )

select * from renamed 
