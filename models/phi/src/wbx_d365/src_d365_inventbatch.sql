with
    d365_source as (
        select *
        from {{ source("D365", "invent_batch") }} where  _FIVETRAN_DELETED='FALSE' and upper(trim(data_area_id)) in {{env_var("DBT_D365_COMPANY_FILTER")}} 
    ),

    renamed as (

       
        select
            'D365' as source,
            invent_batch_id as inventbatchid,
            exp_date as expdate,
            item_id as itemid,
            prod_date as proddate,
            null as description,
            pds_best_before_date as pdsbestbeforedate,
            null as pdscountryoforigin1,
            null as pdscountryoforigin2,
            pds_disposition_code as pdsdispositioncode,
            pds_finished_goods_date_tested as pdsfinishedgoodsdatetested,
            pdsinherit_batch_attrib as pdsinheritbatchattrib,
            pdsinherited_shelf_life as pdsinheritedshelflife,
            pds_same_lot as pdssamelot,
            pds_shelf_advice_date as pdsshelfadvicedate,
            pds_use_vend_batch_date as pdsusevendbatchdate,
            pds_use_vend_batch_exp as pdsusevendbatchexp,
            pds_vend_batch_date as pdsvendbatchdate,
            null as pdsvendbatchid,
            pds_vend_expiry_date as pdsvendexpirydate,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid

        from d365_source

    )

select *
from renamed
