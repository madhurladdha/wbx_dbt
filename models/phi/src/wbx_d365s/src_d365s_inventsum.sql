with
d365source as (
    select *
    from {{ source("D365S", "inventsum") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (

    select
        'D365S' as source,
        itemid as itemid,
        postedqty as postedqty,
        postedvalue as postedvalue,
        deducted as deducted,
        received as received,
        reservphysical as reservphysical,
        reservordered as reservordered,
        onorder as onorder,
        ordered as ordered,
        quotationissue as quotationissue,
        quotationreceipt as quotationreceipt,
        inventdimid as inventdimid,
        closed as closed,
        registered as registered,
        picked as picked,
        availordered as availordered,
        availphysical as availphysical,
        physicalvalue as physicalvalue,
        arrived as arrived,
        physicalinvent as physicalinvent,
        closedqty as closedqty,
        cast(lastupddatephysical as TIMESTAMP_NTZ) as lastupddatephysical,
        cast(lastupddateexpected as TIMESTAMP_NTZ) as lastupddateexpected,
        postedvalueseccur_ru as postedvalueseccur_ru,
        physicalvalueseccur_ru as physicalvalueseccur_ru,
        pdscwarrived as pdscwarrived,
        pdscwavailordered as pdscwavailordered,
        pdscwavailphysical as pdscwavailphysical,
        pdscwdeducted as pdscwdeducted,
        pdscwonorder as pdscwonorder,
        pdscwordered as pdscwordered,
        pdscwphysicalinvent as pdscwphysicalinvent,
        pdscwpicked as pdscwpicked,
        pdscwpostedqty as pdscwpostedqty,
        pdscwquotationissue as pdscwquotationissue,
        pdscwquotationreceipt as pdscwquotationreceipt,
        pdscwreceived as pdscwreceived,
        pdscwregistered as pdscwregistered,
        pdscwreservordered as pdscwreservordered,
        pdscwreservphysical as pdscwreservphysical,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365source

)

select * from renamed