with
    d365_source as (
        select *
        from {{ source("D365", "price_disc_table") }} where _FIVETRAN_DELETED='FALSE' AND upper(data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}
    ),


    renamed as (


        select
            'D365' as source,
            null as agreement,
            item_code as itemcode,
            account_code as accountcode,
            item_relation as itemrelation,
            account_relation as accountrelation,
            quantity_amount_from as quantityamountfrom,
            from_date as fromdate,
            to_date as todate,
            amount as amount,
            currency as currency,
            percent_1 as percent1,
            percent_2 as percent2,
            delivery_time as deliverytime,
            search_again as searchagain,
            price_unit as priceunit,
            relation as relation,
            quantity_amount_to as quantityamountto,
            unit_id as unitid,
            markup as markup,
            allocate_markup as allocatemarkup,
            module as module,
            invent_dim_id as inventdimid,
            calendar_days as calendardays,
            generic_currency as genericcurrency,
            mcrprice_disc_group_type as mcrpricediscgrouptype,
            mcrfixed_amount_cur as mcrfixedamountcur,
            null as mcrmerchandisingeventid,
            agreement_header_ext_ru as agreementheaderext_ru,
            disregard_lead_time as disregardleadtime,
            invent_bailee_free_days_ru as inventbaileefreedays_ru,
            maximum_retail_price_in as maximumretailprice_in,
            original_price_disc_adm_trans_rec_id as originalpricediscadmtransrecid,
            null as pdscalculationid,
            modifieddatetime as modifieddatetime,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid,
            null as wbxfixedexchangerate

        from d365_source

    )

select *
from renamed
