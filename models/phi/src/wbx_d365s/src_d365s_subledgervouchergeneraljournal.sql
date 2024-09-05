with d365_source as (

    select *
    from {{ source("D365S", "subledgervouchergeneraljournalentry") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        'D365S' as source,
        voucher as voucher,
        upper(voucherdataareaid) as voucherdataareaid,
        generaljournalentry as generaljournalentry,
        cast(accountingdate as TIMESTAMP_NTZ) as accountingdate,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source
    where upper(voucherdataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}

)

select *
from renamed
