
with
d365_source as (
    select *
    from {{ source("D365S", "dimensionattributevaluecombination") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365S' as source,
        recid as recid,
        null as _sys_row_id,
        null as data_lake_modified_date_time,
        accountstructure as account_structure,
        displayvalue as display_value,
        ledgerdimensiontype as ledger_dimension_type,
        hashversion as hash_version,
        mainaccount as main_account,
        cast(mainaccountvalue as number) as main_account_value,
        partition as partition,
        recversion as recversion,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        modifiedby as modifiedby,
        cast(createddatetime  as TIMESTAMP_NTZ) as createddatetime,
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
        cast(spaccountvalue as number) as spaccountvalue,
        _fivetran_deleted as _fivetran_deleted,
        _fivetran_synced as _fivetran_synced,
        null as last_processed_change_date_time,
        systemgeneratedattributeprojectvalue
            as systemgeneratedattributeprojectvalue,
        systemgeneratedattributefixedassetvalue
            as systemgeneratedattributefixedassetvalue
    from d365_source

)

select * from renamed
