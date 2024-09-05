with
    d365_source as (

        select *
        from {{ source("D365", "agreement_line_released_line") }} where _FIVETRAN_DELETED='FALSE'
    ),

    renamed as (

        select
            'D365' as source,
            agreement_line as agreementline,
            reference_relation_type as referencerelationtype,
            is_deleted as isdeleted,
            is_modified as ismodified,
            cust_invoice_trans as custinvoicetrans,
            proj_invoice_item as projinvoiceitem,
            purch_line_invent_trans_id as purchlineinventtransid,
            upper(purch_line_data_area_id) as purchlinedataareaid,
            null as saleslineinventtransid,
            null as saleslinedataareaid,
            vend_invoice_trans as vendinvoicetrans,
            recversion as recversion,
            partition as partition,
            recid as recid
        from d365_source --where upper(purchlinedataareaid) in {{env_var("DBT_D365_COMPANY_FILTER")}}
        /*Not applying the dataarea filter for this model as the possible fields are sparsely populated.  Should get filtered as needed based on downstream joins.*/

    )

select * from renamed 