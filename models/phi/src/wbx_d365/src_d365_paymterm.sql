with d365_source as (
        select *
        from {{ source("D365", "paym_term") }} where _FIVETRAN_DELETED='FALSE' AND upper(data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}

    ),

    renamed as (
select
            'D365' as source,
            paym_term_id as paymtermid,
            paym_method as paymmethod,
            num_of_days as numofdays,
            description as description,
            num_of_months as numofmonths,
            null as paymsched,
            cash as cash,
            null as paymdayid,
            ship_carrier_ancillary_charge as shipcarrierancillarycharge,
            post_offsetting_ar as postoffsettingar,
            ship_carrier_certified_check as shipcarriercertifiedcheck,
            credit_card_payment_type as creditcardpaymenttype,
            credit_card_credit_check as creditcardcreditcheck,
            cash_ledger_dimension as cashledgerdimension,
            use_delivery_date_for_due_date_es as usedeliverydateforduedate_es,
            null as duedatelimitgroupid_es,
            use_empl_account_ru as useemplaccount_ru,
            additional_months as additionalmonths,
            customer_update_due_date as customerupdateduedate,
            cut_off_day as cutoffday,
            default_paym_term_psn as defaultpaymterm_psn,
            vendor_update_due_date as vendorupdateduedate,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid,
            cfmpayment_request_type_payment as cfmpaymentrequesttypepayment,
            cfmpayment_request_type_prepayment as cfmpaymentrequesttypeprepaym

        from d365_source

    )

select * from renamed 
