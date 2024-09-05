

with source as (

    select * from {{ source('WEETABIX', 'forecastsales') }}

),

renamed as (

    select
        itemid,
        startdate,
        enddate,
        freqcode,
        active,
        inventqty,
        salesprice,
        discpercent,
        comment_,
        custgroupid,
        itemgroupid,
        custaccountid,
        keyid,
        currency,
        expandid,
        report,
        salesqty,
        salesunitid,
        salesmarkup,
        discamount,
        priceunit,
        costprice,
        taxitemgroupid,
        cov,
        covstatus,
        itemallocateid,
        taxgroupid,
        freq,
        amount,
        modelid,
        allocatemethod,
        itembomid,
        itemrouteid,
        defaultdimension,
        projid,
        projcategoryid,
        projlinepropertyid,
        inventdimid,
        projtransid,
        projforecastsalespaymdate,
        projforecastcostpaymdate,
        projforecastinvoicedate,
        projforecasteliminationdate,
        activitynumber,
        projforecastbudgettype,
        projfundingsource,
        psarefpurchline,
        pdscwqty,
        pdscwunitid,
        modifiedby,
        dataareaid,
        recversion,
        partition,
        recid,
        wbxforecastdifference,
        wbxforecastallocationpct

    from source

)

select * from renamed
