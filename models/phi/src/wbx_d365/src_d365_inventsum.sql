with
    d365_source as (
        select *
        from {{ source("D365", "invent_sum") }} where _FIVETRAN_DELETED='FALSE' and  upper(data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}
    ),

    renamed as (

        select
            'D365' as source,
            item_id as itemid,
            posted_qty as postedqty,
            posted_value as postedvalue,
            deducted as deducted,
            received as received,
            reserv_physical as reservphysical,
            reserv_ordered as reservordered,
            on_order as onorder,
            ordered as ordered,
            quotation_issue as quotationissue,
            quotation_receipt as quotationreceipt,
            invent_dim_id as inventdimid,
            closed as closed,
            registered as registered,
            picked as picked,
            avail_ordered as availordered,
            avail_physical as availphysical,
            physical_value as physicalvalue,
            arrived as arrived,
            physical_invent as physicalinvent,
            closed_qty as closedqty,
            last_upd_date_physical as lastupddatephysical,
            last_upd_date_expected as lastupddateexpected,
            posted_value_sec_cur_ru as postedvalueseccur_ru,
            physical_value_sec_cur_ru as physicalvalueseccur_ru,
            pds_cwarrived as pdscwarrived,
            pds_cwavail_ordered as pdscwavailordered,
            pds_cwavail_physical as pdscwavailphysical,
            pds_cwdeducted as pdscwdeducted,
            pds_cwon_order as pdscwonorder,
            pds_cwordered as pdscwordered,
            pds_cwphysical_invent as pdscwphysicalinvent,
            pds_cwpicked as pdscwpicked,
            pds_cwposted_qty as pdscwpostedqty,
            pds_cwquotation_issue as pdscwquotationissue,
            pds_cwquotation_receipt as pdscwquotationreceipt,
            pds_cwreceived as pdscwreceived,
            pds_cwregistered as pdscwregistered,
            pds_cwreserv_ordered as pdscwreservordered,
            pds_cwreserv_physical as pdscwreservphysical,
            modifieddatetime as modifieddatetime,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid
        from d365_source  

    )

select * from renamed