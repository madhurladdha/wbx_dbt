with d365_source as (
        select *
        from {{ source("D365S", "whsworkquarantine") }}
        where _FIVETRAN_DELETED='FALSE' and trim(upper(dataareaid)) in {{env_var("DBT_D365_COMPANY_FILTER")}}
),

renamed as (

    select
        'D365' as source,
        workid as workid,
        linenum as linenum,
        itemid as itemid,
        inventtransidfrom as inventtransidfrom,
        inventtransidto as inventtransidto,
        qty as qty,
        inventqtyremain as inventqtyremain,
        inventdimidfrom as inventdimidfrom,
        inventdimidto as inventdimidto,
        transdatetime as transdatetime,
        null as transdatetimetzid,
        modifieddatetime as modifieddatetime,
        modifiedby as modifiedby,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid,
        null as wbxreasoncode

    from d365_source

)

select * from renamed

