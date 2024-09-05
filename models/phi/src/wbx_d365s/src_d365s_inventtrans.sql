with
d365source as (
    select *
    from {{ source("D365S", "inventtrans") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}

),

renamed as (

    select
        'D365S' as source,
        itemid as itemid,
        statusissue as statusissue,
        intercompanyinventdimtransferred as intercoinventdimtransferred,
        cast(datephysical as TIMESTAMP_NTZ) as datephysical,
        qty as qty,
        costamountposted as costamountposted,
        currencycode as currencycode,
        invoiceid as invoiceid,
        voucher as voucher,
        cast(dateexpected as TIMESTAMP_NTZ) as dateexpected,
        cast(datefinancial as TIMESTAMP_NTZ) as datefinancial,
        costamountphysical as costamountphysical,
        statusreceipt as statusreceipt,
        packingslipreturned as packingslipreturned,
        invoicereturned as invoicereturned,
        packingslipid as packingslipid,
        voucherphysical as voucherphysical,
        costamountadjustment as costamountadjustment,
        cast(shippingdaterequested as TIMESTAMP_NTZ) as shippingdaterequested,
        cast(shippingdateconfirmed as TIMESTAMP_NTZ) as shippingdateconfirmed,
        qtysettled as qtysettled,
        costamountsettled as costamountsettled,
        valueopen as valueopen,
        null as activitynumber,
        cast(datestatus as TIMESTAMP_NTZ) as datestatus,
        costamountstd as costamountstd,
        cast(dateclosed as TIMESTAMP_NTZ) as dateclosed,
        pickingrouteid as pickingrouteid,
        costamountoperations as costamountoperations,
        returninventtransorigin as returninventtransorigin,
        null as projid,
        null as projcategoryid,
        inventdimid as inventdimid,
        markingrefinventtransorigin as markingrefinventtransorigin,
        inventdimfixed as inventdimfixed,
        cast(dateinvent as TIMESTAMP_NTZ) as dateinvent,
        transchildrefid as transchildrefid,
        transchildtype as transchildtype,
        timeexpected as timeexpected,
        revenueamountphysical as revenueamountphysical,
        null as projadjustrefid,
        taxamountphysical as taxamountphysical,
        inventtransorigin as inventtransorigin,
        storno_ru as storno_ru,
        stornophysical_ru as stornophysical_ru,
        null as inventdimidsales_ru,
        groupreftype_ru as groupreftype_ru,
        null as grouprefid_ru,
        costamountseccurposted_ru as costamountseccurposted_ru,
        costamountseccurphysical_ru as costamountseccurphysical_ru,
        costamountseccuradjustment_ru as costamountseccuradjustment_ru,
        cast(dateclosedseccur_ru as TIMESTAMP_NTZ) as dateclosedseccur_ru,
        qtysettledseccur_ru as qtysettledseccur_ru,
        costamountsettledseccur_ru as costamountsettledseccur_ru,
        valueopenseccur_ru as valueopenseccur_ru,
        costamountstdseccur_ru as costamountstdseccur_ru,
        inventtransorigindelivery_ru as inventtransorigindelivery_ru,
        inventtransoriginsales_ru as inventtransoriginsales_ru,
        inventtransorigintransit_ru as inventtransorigintransit_ru,
        pdscwqty as pdscwqty,
        pdscwsettled as pdscwsettled,
        nonfinancialtransferinventclosing as nonfinancialtransferinventclos,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid,
        null as whsuser,
        null as modifiedby,
        null as createddatetime
    from d365source

)

select * from renamed
