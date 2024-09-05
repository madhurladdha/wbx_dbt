
with source as (

    select * from {{ source('EI_RDM', 'ref_normalization_xref') }}

),

renamed as (

    select
        reference_field,
        lookup_category,
        source_value,
        source_system,
        normalized_value,
        field_comments,
        valid_flag,
        load_date,
        loaded_by,
        update_date,
        updated_by,
        delete_flag

    from source

)

select * from renamed