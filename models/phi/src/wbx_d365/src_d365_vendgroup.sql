with d365_source as (
        select *
        from {{ source("D365", "vend_group") }} where _FIVETRAN_DELETED='FALSE' and upper(data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}

    ),

renamed as (

    select  
        'D365' as source,
        vend_group as vendgroup,
        name as name,
        null as clearingperiod,
        null as paymtermid,
        null as taxgroupid,
        null as taxperiodpaymentcode_pl,
        exclude_from_signup_psn as excludefromsignup_psn,
        createddatetime as createddatetime,
        null as del_createdtime,
        createdby as createdby,
        upper(data_area_id) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid
    
    from d365_source

)

select * from renamed 
