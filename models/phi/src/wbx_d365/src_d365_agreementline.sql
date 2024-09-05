with
    d365_source as (

        select * from {{ source("D365", "agreement_line") }} where _FIVETRAN_DELETED='FALSE'
    ),

    renamed as (
        select
            'D365' as source,
            null as commitedamount,
            commitedquantity as commitedquantity,
            productunitofmeasure as productunitofmeasure,
            priceunit as priceunit,
            priceperunit as priceperunit,
            linediscountamount as linediscountamount,
            pdscwcommitedquantity as pdscwcommitedquantity,
            instance_relation_type as instancerelationtype,
            line_number as linenumber,
            agreement_line_type as agreementlinetype,
            agreement_line_product as agreementlineproduct,
            expiration_date as expirationdate,
            effective_date as effectivedate,
            line_discount_percent as linediscountpercent,
            agreed_release_line_min_amount as agreedreleaselineminamount,
            agreed_release_line_max_amount as agreedreleaselinemaxamount,
            is_price_information_mandatory as ispriceinformationmandatory,
            is_max_enforced as ismaxenforced,
            is_deleted as isdeleted,
            is_modified as ismodified,
            category as category,
            item_id as itemid,
            agreement as agreement,
            upper(item_data_area_id) as itemdataareaid,
            invent_dim_id as inventdimid,
            upper(invent_dim_data_area_id) as inventdimdataareaid,
            null as projectprojid,
            null as projectdataareaid,
            default_dimension  as defaultdimension,
            currency as currency,
            recversion as recversion,
            relationtype as relationtype,
            partition as partition,
            recid as recid
        from d365_source where upper(itemdataareaid) in {{env_var("DBT_D365_COMPANY_FILTER")}}

    )

select * from renamed
