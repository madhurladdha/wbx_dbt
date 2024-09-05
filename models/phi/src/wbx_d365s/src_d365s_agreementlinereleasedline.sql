with
d365_source as (

    select *
    from {{ source("D365S", "agreementlinereleasedline") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365S' as source,
        agreementline as agreementline,
        referencerelationtype as referencerelationtype,
        isdeleted as isdeleted,
        ismodified as ismodified,
        custinvoicetrans as custinvoicetrans,
        projinvoiceitem as projinvoiceitem,
        purchlineinventtransid as purchlineinventtransid,
        upper(purchlinedataareaid) as purchlinedataareaid,
        null as saleslineinventtransid,
        null as saleslinedataareaid,
        vendinvoicetrans as vendinvoicetrans,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source 
    --where upper(purchlinedataareaid) in ('RFL','IBE','WBX')
/*Not applying the dataarea filter for this model as the possible fields are sparsely populated. Should get filtered as needed based on downstream joins.*/

)

select * from renamed