with
    d365_source as (

        select *
        from {{ source("D365", "cust_settlement") }} where upper(trim(data_area_id)) in {{env_var("DBT_D365_COMPANY_FILTER")}} and _FIVETRAN_DELETED='FALSE'
    ),

    renamed as (


        select
            'D365' as source,
            trans_rec_id  as transrecid,
            trans_date as transdate,
            offset_trans_voucher as offsettransvoucher,
            account_num as accountnum,
            settle_amount_cur as settleamountcur,
            settle_amount_mst as settleamountmst,
            exch_adjustment as exchadjustment,
            null as settlementvoucher,
            trans_company as transcompany,
            offset_recid   as offsetrecid,
            due_date as duedate,
            utilized_cash_disc as utilizedcashdisc,
            cust_cash_disc_date as custcashdiscdate,
            last_interest_date as lastinterestdate,
            penny_diff as pennydiff,
            can_be_reversed as canbereversed,
            settle_tax_1099_amount as settletax1099amount,
            settle_tax_1099_state_amount as settletax1099stateamount,
            default_dimension as defaultdimension,
            cash_discount_ledger_dimension as cashdiscountledgerdimension,
            null as eusaleslist,
            offset_company as offsetcompany,
            offset_account_num as offsetaccountnum,
            settlement_group as settlementgroup,
            settle_amount_reporting as settleamountreporting,
            exch_adjustment_reporting as exchadjustmentreporting,
            interest_amount_br as interestamount_br,
            fine_amount_br as fineamount_br,
            null as interestcode_br,
            null as finecode_br,
            null as taxvoucher_ru,
            reversed_rec_id_ru as reversedrecid_ru,
            reverse_trans_ru as reversetrans_ru,
            reporting_date_ru as reportingdate_ru,
            createddatetime as createddatetime,
            null as del_createdtime,
            createdby as createdby,
            upper(trim(data_area_id)) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid
        from d365_source
    )

select * from renamed
