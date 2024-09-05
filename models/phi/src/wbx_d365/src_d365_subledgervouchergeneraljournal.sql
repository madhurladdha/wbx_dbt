with d365_source as (

    select *
    from {{ source("D365", "subledger_voucher_general_journal_entry") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        'D365' as source,
        voucher as voucher,
        upper(voucher_data_area_id) as voucherdataareaid,
        general_journal_entry as generaljournalentry,
        accounting_date as accountingdate,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source where upper(voucherdataareaid)  in {{env_var("DBT_D365_COMPANY_FILTER")}}

)

select *
from renamed
