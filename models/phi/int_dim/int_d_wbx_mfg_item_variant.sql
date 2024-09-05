{{ config(tags=["manufacturing", "supply_schedule", "item_variant", "dim", "wbx"]) }}

with
    src_stg as (select * from {{ ref("stg_d_wbx_mfg_item_variant") }}),
    source as (
        select
            '{{env_var("DBT_SOURCE_SYSTEM")}}' as source_system,
            source_item_identifier as source_item_identifier,
            {{
                dbt_utils.surrogate_key(
                    [
                        "source_system",
                        "source_item_identifier",
                    ]
                )
            }} as item_guid,
            company_code,
            variant_code as variant_code,
            variant_desc,
            variant_status,
            item_allocation_key as item_allocation_key,

            effective_date as effective_date,
            expiration_date as expiration_date,
            effective_date as new_effective_date,
            expiration_date as new_expiration_date,
            source_item_identifier as new_source_item_identifier,
            variant_code as new_variant_code,
            variant_code as prev_variant_code,
            item_allocation_key as new_item_allocation_key,
            item_allocation_key as prev_item_allocation_key,
            expiration_date as prev_expiration_date,
            effective_date as prev_effective_date,
            source_item_identifier as prev_source_item_identifier,
            dense_rank() over (
                partition by source_item_identifier, variant_code, item_allocation_key
                order by
                    variant_code,
                    item_allocation_key,
                    source_item_identifier,
                    effective_date,
                    expiration_date
            ) as rownu,
            rownu as variant_eff_version_no,
            active_flag,
            source_update_date as source_updated_date,
            current_timestamp() as load_date,
            current_timestamp() as update_date

        from src_stg
    ),
    tfm as (
        select
            cast(substring(source_system, 1, 255) as text(255)) as source_system,

            cast(
                substring(source_item_identifier, 1, 255) as text(255)
            ) as source_item_identifier,

            cast(item_guid as text(255)) as item_guid,

            cast(substring(company_code, 1, 255) as text(255)) as company_code,

            cast(substring(variant_code, 1, 255) as text(255)) as variant_code,

            cast(substring(variant_desc, 1, 510) as text(510)) as variant_desc,

            cast(variant_status as number(10, 0)) as variant_status,

            cast(
                substring(item_allocation_key, 1, 255) as text(255)
            ) as item_allocation_key,

            cast(variant_eff_version_no as number(10, 0)) as variant_eff_version_no,

            cast(effective_date as timestamp_ntz(9)) as effective_date,

            cast(expiration_date as timestamp_ntz(9)) as expiration_date,

            cast(substring(active_flag, 1, 255) as text(255)) as active_flag,

            cast(source_updated_date as timestamp_ntz(9)) as source_updated_date,

            cast(load_date as timestamp_ntz(9)) as load_date,

            cast(update_date as timestamp_ntz(9)) as update_date

        -- cast(unique_key as text(255) ) as unique_key
        from source
    ),

    final as (
        select
            t.*,
            {{
                dbt_utils.surrogate_key(
                    [
                        "SOURCE_SYSTEM",
                        "SOURCE_ITEM_IDENTIFIER",
                        "COMPANY_CODE",
                        "VARIANT_CODE",
                        "EFFECTIVE_DATE",
                        "EXPIRATION_DATE",
                        "ACTIVE_FLAG",
                    ]
                )
            }} as unique_key
        from tfm t
    )

select *
from final
