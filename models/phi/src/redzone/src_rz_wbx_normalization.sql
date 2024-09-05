{{ config(      materialized='view',tags=["redzone","OEE", 'v_losses', 'v_run', 'v_shift', 'choke point', 'normalization'],    ) }}

with
    source as (select * from {{ source("REDZONE", "RZ_WBX_NORMALIZATION") }}),

    renamed as (

        select
            reference_field,
            lookup_category,
            source_value,
            source_site,
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

select *
from renamed
