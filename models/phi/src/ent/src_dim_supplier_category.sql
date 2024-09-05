

with source as (

    select * from {{ source('DIM_ENT', 'dim_supplier_category') }}

),

renamed as (

    select
        unique_key,
        source_system_address_number,
        source_system,
        supplier_name,
        company,
        unified_supplier_name_ml_score,
        category_level2_ml_derived,
        unified_supplier_name_ml,
        supplier_type_ml_derived,
        unified_supplier_name,
        category_level1_ml,
        category_level1,
        category_level2,
        category_level3,
        category_level4,
        category_level5,
        usn_custom_etl,
        supplier_type,
        update_date,
        updated_by,
        comment

    from source

)

select * from renamed

