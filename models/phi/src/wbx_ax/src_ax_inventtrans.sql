

with source as (

    select * from {{ source('WEETABIX', 'inventtrans') }}

),

renamed as (

    select
        itemid,
        statusissue,
        intercoinventdimtransferred,
        datephysical,
        qty,
        costamountposted,
        currencycode,
        invoiceid,
        voucher,
        dateexpected,
        datefinancial,
        costamountphysical,
        statusreceipt,
        packingslipreturned,
        invoicereturned,
        packingslipid,
        voucherphysical,
        costamountadjustment,
        shippingdaterequested,
        shippingdateconfirmed,
        qtysettled,
        costamountsettled,
        valueopen,
        activitynumber,
        datestatus,
        costamountstd,
        dateclosed,
        pickingrouteid,
        costamountoperations,
        returninventtransorigin,
        projid,
        projcategoryid,
        inventdimid,
        markingrefinventtransorigin,
        inventdimfixed,
        dateinvent,
        transchildrefid,
        transchildtype,
        timeexpected,
        revenueamountphysical,
        projadjustrefid,
        taxamountphysical,
        inventtransorigin,
        storno_ru,
        stornophysical_ru,
        inventdimidsales_ru,
        groupreftype_ru,
        grouprefid_ru,
        costamountseccurposted_ru,
        costamountseccurphysical_ru,
        costamountseccuradjustment_ru,
        dateclosedseccur_ru,
        qtysettledseccur_ru,
        costamountsettledseccur_ru,
        valueopenseccur_ru,
        costamountstdseccur_ru,
        inventtransorigindelivery_ru,
        inventtransoriginsales_ru,
        inventtransorigintransit_ru,
        pdscwqty,
        pdscwsettled,
        nonfinancialtransferinventclos,
        modifieddatetime,
        dataareaid,
        recversion,
        partition,
        recid,
        whsuser,
        modifiedby,
        createddatetime

    from source

)

select * from renamed
