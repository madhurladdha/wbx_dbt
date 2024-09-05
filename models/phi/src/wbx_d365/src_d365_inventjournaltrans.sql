with
    d365_source as (
        select *
        from {{ source("D365", "invent_journal_trans") }} where _FIVETRAN_DELETED='FALSE' and upper(trim(data_area_id)) in {{env_var("DBT_D365_COMPANY_FILTER")}} 
    ),

    renamed as (

        select
            'D365' as source,
            journal_id as journalid,
            line_num as linenum,
            trans_date as transdate,
            voucher as voucher,
            journal_type as journaltype,
            item_id as itemid,
            qty as qty,
            cost_price as costprice,
            price_unit as priceunit,
            cost_markup as costmarkup,
            cost_amount as costamount,
            sales_amount as salesamount,
            null as projtransid,
            invent_trans_id as inventtransid,
            null as inventtransidfather,
            invent_on_hand as inventonhand,
            counted as counted,
            bomline as bomline,
            null as inventtransidreturn,
            null as projcategoryid,
            null as projid,
            to_invent_trans_id as toinventtransid,
            reason_ref_rec_id as reasonrefrecid,
            invent_dim_id as inventdimid,
            to_invent_dim_id as toinventdimid,
            null as reqpoid,
            asset_trans_type as assettranstype,
            null as assetid,
            null as assetbookid,
            null as projtaxgroupid,
            null as projsalescurrencyid,
            null as projlinepropertyid,
            null as projtaxitemgroupid,
            null as projunitid,
            proj_sales_price as projsalesprice,
            invent_ref_type as inventreftype,
            null as inventrefid,
            null as inventreftransid,
            profit_set as profitset,
            null as activitynumber,
            release_date as releasedate,
            releasedatetzid as releasedatetzid,
            ledger_dimension  as ledgerdimension,
            worker  as worker,
            default_dimension  as defaultdimension,
            null as excisetariffcodes_in,
            null as excisetype_in,
            null as exciserecordtype_in,
            null as dsa_in,
            storno_ru as storno_ru,
            intrastat_fulfillment_date_hu as intrastatfulfillmentdate_hu,
            null as scraptypeid_ru,
            null as retailinfocodeidex2,
            null as retailinformationsubcodeidex2,
            pds_copy_batch_attrib as pdscopybatchattrib,
            pds_cwinvent_on_hand as pdscwinventonhand,
            pds_cwinvent_qty_counted as pdscwinventqtycounted,
            pds_cwqty as pdscwqty,
            null as postaladdress_in,
            null as warehouselocation_in,
            modifieddatetime as modifieddatetime,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid

        from d365_source

    )

select *
from renamed 