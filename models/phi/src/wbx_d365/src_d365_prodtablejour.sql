with
    d365_source as (
        select *
        from {{ source("D365", "prod_table_jour") }} where _FIVETRAN_DELETED='FALSE' AND upper(data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}
  
    ),

    renamed as (

       

        select
            'D365' as source,
            prod_id as prodid,
            item_id as itemid,
            invent_trans_id as inventtransid,
            trans_date as transdate,
            journal_type as journaltype,
            qty_good as qtygood,
            voucher as voucher,
            amount_financial as amountfinancial,
            adjustment as adjustment,
            qty_error as qtyerror,
            amount_physical as amountphysical,
            null as scrapvoucher,
            canceled as canceled,
            open_prod_order as openprodorder,
            amount_financial_sec_cur_ru as amountfinancialseccur_ru,
            pds_cwbatch_err as pdscwbatcherr,
            pds_cwbatch_good as pdscwbatchgood,
            modifieddatetime as modifieddatetime,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid

        from d365_source

    )

select * from renamed


