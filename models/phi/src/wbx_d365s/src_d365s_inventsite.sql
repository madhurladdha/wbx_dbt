
with

    d365_source as (
        select *
        from {{ source("D365S", "inventsite") }} where _FIVETRAN_DELETED='FALSE' and upper(trim(dataareaid)) in {{env_var("DBT_D365_COMPANY_FILTER")}} 

    ),

    renamed as (

        select
            'D365S' as source,
            siteid as siteid,
            name as name,
            defaultdimension as defaultdimension,
            timezone as timezone,
            null as orderentrydeadlinegroupid,
            defaultinventstatusid as defaultinventstatusid,
            taxbranchrefrecid as taxbranchrefrecid,
            isreceivingwarehouseoverrideallowed as isreceivingwarehouseovrdealwd,
            upper(dataareaid) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid

        from d365_source

    )

select *
from renamed
