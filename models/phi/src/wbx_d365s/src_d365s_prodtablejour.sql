with
d365_source as (
    select *
    from {{ source("D365S", "prodtablejour") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}

),

renamed as (



    select
        'D365S' as source,
        prodid as prodid,
        itemid as itemid,
        inventtransid as inventtransid,
        cast(transdate as TIMESTAMP_NTZ) as transdate,
        journaltype as journaltype,
        qtygood as qtygood,
        voucher as voucher,
        amountfinancial as amountfinancial,
        adjustment as adjustment,
        qtyerror as qtyerror,
        amountphysical as amountphysical,
        null as scrapvoucher,
        canceled as canceled,
        openprodorder as openprodorder,
        amountfinancialseccur_ru as amountfinancialseccur_ru,
        pdscwbatcherr as pdscwbatcherr,
        pdscwbatchgood as pdscwbatchgood,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed


