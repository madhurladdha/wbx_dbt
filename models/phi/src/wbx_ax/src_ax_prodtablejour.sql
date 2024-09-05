

with source as (

    select * from {{ source('WEETABIX', 'prodtablejour') }}

),

renamed as (

    select
        prodid,
        itemid,
        inventtransid,
        transdate,
        journaltype,
        qtygood,
        voucher,
        amountfinancial,
        adjustment,
        qtyerror,
        amountphysical,
        scrapvoucher,
        canceled,
        openprodorder,
        amountfinancialseccur_ru,
        pdscwbatcherr,
        pdscwbatchgood,
        modifieddatetime,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
