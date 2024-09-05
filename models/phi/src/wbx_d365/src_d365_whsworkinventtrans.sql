with d365_source as (
        select *
        from {{ source("D365", "whswork_invent_trans") }} where _FIVETRAN_DELETED='FALSE' and upper(data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}
    ),

    renamed as (


        select
            'D365' as source,
            work_id as workid,
            line_num as linenum,
            invent_trans_id_from as inventtransidfrom,
            invent_trans_id_to as inventtransidto,
            item_id as itemid,
            invent_dim_id_from as inventdimidfrom,
            invent_dim_id_to as inventdimidto,
            invent_trans_id_parent as inventtransidparent,
            qty as qty,
            invent_qty_remain as inventqtyremain,
            work_has_reservation as workhasreservation,
            trans_date_time as transdatetime,
            transdatetimetzid as transdatetimetzid,
            modifieddatetime as modifieddatetime,
            modifiedby as modifiedby,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid

        from d365_source  

    )

select *
from renamed
