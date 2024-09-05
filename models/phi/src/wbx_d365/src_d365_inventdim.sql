
    with d365_source as (
        select *
        from {{ source("D365", "invent_dim") }} where _FIVETRAN_DELETED='FALSE' and upper(trim(data_area_id)) in {{env_var("DBT_D365_COMPANY_FILTER")}} 
    ),

    renamed as (

        select
            'D365' as source,
            invent_dim_id as inventdimid,
            invent_batch_id as inventbatchid,
            w_mslocation_id as wmslocationid,
            null as wmspalletid,
            null as inventserialid,
            invent_location_id as inventlocationid,
            config_id as configid,
            invent_size_id as inventsizeid,
            null as inventcolorid,
            invent_site_id as inventsiteid,
            null as inventgtdid_ru,
            null as inventprofileid_ru,
            null as inventownerid_ru,
            null as inventstyleid,
            license_plate_id as licenseplateid,
            invent_status_id as inventstatusid,
            null as sha1hash,
            modifieddatetime as modifieddatetime,
            modifiedby as modifiedby,
            createddatetime as createddatetime,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid

        from d365_source

    )

select *
from renamed 
