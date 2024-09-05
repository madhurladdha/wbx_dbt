/*  29-May-2023: repointing the main source from EI_RDM.itm_procurement_category_ext_stg to DIM_ENT.itm_procurement_category_ext_stg
*/
with source as (

    select * from {{ source('DIM_ENT', 'itm_procurement_category_ext_stg') }}

),

renamed as (

    select
        source_system,
        case_item_number,
        source_item_identifier,
        item_guid,
        item_description,
        item_type,
        highlevel_category_code,
        midlevel_category_code,
        lowlevel_category_code,
        master_reporting_category,
        alternate_reporting_category,
        buyer_name,
        item_category_1,
        item_category_2,
        item_category_3,
        item_category_4,
        item_category_5,
        item_category_6,
        item_category_7,
        item_category_8,
        item_category_9,
        item_category_10,
        null as updated_by,
        update_date

    from source

)

select * from renamed