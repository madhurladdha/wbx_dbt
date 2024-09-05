with d365_source as (
        select *
        from {{ source("D365S", "paymterm") }} where _FIVETRAN_DELETED='FALSE' AND upper(dataareaid) in {{env_var("DBT_D365_COMPANY_FILTER")}}

    ),

    renamed as (
select
            'D365S' as source,
            paymtermid as paymtermid,
            paymmethod as paymmethod,
            numofdays as numofdays,
            description as description,
            numofmonths as numofmonths,
            null as paymsched,
            cash as cash,
            null as paymdayid,
            shipcarrierancillarycharge as shipcarrierancillarycharge,
            postoffsettingar as postoffsettingar,
            shipcarriercertifiedcheck as shipcarriercertifiedcheck,
            creditcardpaymenttype as creditcardpaymenttype,
            creditcardcreditcheck as creditcardcreditcheck,
            cashledgerdimension as cashledgerdimension,
            usedeliverydateforduedate_es as usedeliverydateforduedate_es,
            null as duedatelimitgroupid_es,
            useemplaccount_ru as useemplaccount_ru,
            additionalmonths as additionalmonths,
            customerupdateduedate as customerupdateduedate,
            cutoffday as cutoffday,
            defaultpaymterm_psn as defaultpaymterm_psn,
            vendorupdateduedate as vendorupdateduedate,
            upper(dataareaid) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid,
            cfmpaymentrequesttypepayment as cfmpaymentrequesttypepayment,
            cfmpaymentrequesttypeprepayment as cfmpaymentrequesttypeprepaym

        from d365_source

    )

select * from renamed 
