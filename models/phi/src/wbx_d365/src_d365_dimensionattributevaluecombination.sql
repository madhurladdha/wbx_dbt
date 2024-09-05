
with
d365_source as (
    select *
    from {{ source("D365", "dimension_attribute_value_combination") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365' as source,
        recid as recid,
        _sys_row_id as _sys_row_id,
        data_lake_modified_date_time as data_lake_modified_date_time,
        account_structure as account_structure,
        display_value as display_value,
        ledger_dimension_type as ledger_dimension_type,
        hash_version as hash_version,
        main_account as main_account,
        main_account_value as main_account_value,
        partition as partition,
        recversion as recversion,
        modifieddatetime as modifieddatetime,
        modifiedby as modifiedby,
        createddatetime as createddatetime,
        createdby as createdby,
        systemgeneratedattributebankaccount
            as systemgeneratedattributebankaccount,
        systemgeneratedattributebankaccountvalue
            as systemgeneratedattributebankaccountvalue,
        systemgeneratedattributecustomer as systemgeneratedattributecustomer,
        systemgeneratedattributecustomervalue
            as systemgeneratedattributecustomervalue,
        systemgeneratedattributeemployee as systemgeneratedattributeemployee,
        systemgeneratedattributefixedasset
            as systemgeneratedattributefixedasset,
        systemgeneratedattributeitem as systemgeneratedattributeitem,
        systemgeneratedattributeproject as systemgeneratedattributeproject,
        systemgeneratedattributevendor as systemgeneratedattributevendor,
        systemgeneratedattributevendorvalue
            as systemgeneratedattributevendorvalue,
        systemgeneratedattributefixedassets_ru
            as systemgeneratedattributefixedassets_ru,
        systemgeneratedattributerdeferrals
            as systemgeneratedattributerdeferrals,
        systemgeneratedattributercash as systemgeneratedattributercash,
        systemgeneratedattributeemployee_ru
            as systemgeneratedattributeemployee_ru,
        costcenters as costcenters,
        costcentersvalue as costcentersvalue,
        sites as sites,
        sitesvalue as sitesvalue,
        productclass as productclass,
        productclassvalue as productclassvalue,
        usaccount as usaccount,
        usaccountvalue as usaccountvalue,
        spaccount as spaccount,
        spaccountvalue as spaccountvalue,
        _fivetran_deleted as _fivetran_deleted,
        _fivetran_synced as _fivetran_synced,
        last_processed_change_date_time as last_processed_change_date_time,
        systemgeneratedattributeprojectvalue
            as systemgeneratedattributeprojectvalue,
        systemgeneratedattributefixedassetvalue
            as systemgeneratedattributefixedassetvalue
    from d365_source

)

select * from renamed
