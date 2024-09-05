
with

    d365_source as (
        select *
        from {{ source("D365", "invent_site") }} where _FIVETRAN_DELETED='FALSE' and upper(trim(data_area_id)) in {{env_var("DBT_D365_COMPANY_FILTER")}} 

    ),

    renamed as (

        select
            'D365' as source,
            site_id as siteid,
            name as name,
            default_dimension as defaultdimension,
            timezone as timezone,
            null as orderentrydeadlinegroupid,
            default_invent_status_id as defaultinventstatusid,
            tax_branch_ref_rec_id as taxbranchrefrecid,
            is_receiving_warehouse_override_allowed as isreceivingwarehouseovrdealwd,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid

        from d365_source

    )

select *
from renamed
