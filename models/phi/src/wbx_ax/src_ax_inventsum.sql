

with source as (

    select * from {{ source('WEETABIX', 'inventsum') }}

),

renamed as (

    select
        itemid,
        postedqty,
        postedvalue,
        deducted,
        received,
        reservphysical,
        reservordered,
        onorder,
        ordered,
        quotationissue,
        quotationreceipt,
        inventdimid,
        closed,
        registered,
        picked,
        availordered,
        availphysical,
        physicalvalue,
        arrived,
        physicalinvent,
        closedqty,
        lastupddatephysical,
        lastupddateexpected,
        postedvalueseccur_ru,
        physicalvalueseccur_ru,
        pdscwarrived,
        pdscwavailordered,
        pdscwavailphysical,
        pdscwdeducted,
        pdscwonorder,
        pdscwordered,
        pdscwphysicalinvent,
        pdscwpicked,
        pdscwpostedqty,
        pdscwquotationissue,
        pdscwquotationreceipt,
        pdscwreceived,
        pdscwregistered,
        pdscwreservordered,
        pdscwreservphysical,
        modifieddatetime,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
