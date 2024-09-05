{{
    config(
    materialized =env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_dim"]
    )
}}

with
cross_ref as (select * from {{ ref("plant_d365_ref") }}),

ax_plant as (select * from {{ source("WBX_PROD", "dim_wbx_plant_dc") }}),

ref_ax as (
    select
        ax.generic_address_type,
        ax.unique_key,
        ax.source_business_unit_code as source_business_unit_code,
        nvl(
            ref.d365, ax.source_business_unit_code
        ) as source_business_unit_code_new,
        ax.plantdc_address_guid,
            {{
                dbt_utils.surrogate_key(
                    [
                        "ax.SOURCE_SYSTEM",
                        "SOURCE_BUSINESS_UNIT_CODE_NEW",
                        "ax.GENERIC_ADDRESS_TYPE",
                    ]
                )
            }} as plantdc_address_guid_new,
        ax.source_system,
        nvl(ref.name,ax.business_unit_name) as business_unit_name,
        ax.type,
        ax.division,
        ax.region,
        ax.branch_office,
        ax.business_unit_long_description,
        ax.department_type,
        ax.company_code,
        ax.company_name,
        ax.consolidated_shipment_dc_name,
        ax.operating_company,
        ax.segment,
        ax.active_cc_flag,
        ax.date_inserted,
        ax.date_updated,
        ax.etl_exclude_flag,
        case when ref.d365 is not null then 'Y' else 'N' end as conv_status
    from ax_plant as ax
    left join
        cross_ref as ref
        on upper(trim(ax.source_business_unit_code)) = upper(trim(ref.ax))
),

final_conv as (
    select
        {{ dbt_utils.surrogate_key(["PLANTDC_ADDRESS_GUID_NEW"]) }}
            as unique_key_new,
        generic_address_type,
        unique_key as unique_key_old,
        plantdc_address_guid,
        plantdc_address_guid_new,
        source_system,
        source_business_unit_code,
        source_business_unit_code_new,
        business_unit_name,
        type,
        division,
        region,
        branch_office,
        business_unit_long_description,
        department_type,
        company_code,
        company_name,
        consolidated_shipment_dc_name,
        operating_company,
        segment,
        active_cc_flag,
        date_inserted,
        date_updated,
        etl_exclude_flag,
        conv_status
    from ref_ax
),

dim_conv as (
    select distinct
        
        cast(substr(a.unique_key_new, 1, 255) as text(255)) as unique_key,
        cast(
            substr(a.generic_address_type, 1, 255) as text(255)
        ) as generic_address_type,
        cast(
            substr(a.plantdc_address_guid, 1, 255) as text(255)
        ) as plantdc_address_guid,
        cast(
            substr(a.plantdc_address_guid_new, 1, 255) as text(255)
        ) as plantdc_address_guid_new,
        cast(substr(a.source_system, 1, 255) as text(255)) as source_system,
        cast(
            substr(a.source_business_unit_code, 1, 255) as text(255)
        ) as source_business_unit_code,
        cast(
            substr(a.source_business_unit_code_new, 1, 255) as text(255)
        ) as source_business_unit_code_new,
        cast(
            substr(a.business_unit_name, 1, 255) as text(255)
        ) as business_unit_name,
        cast(substr(a.type, 1, 255) as text(255)) as type,
        cast(substr(a.division, 1, 255) as text(255)) as division,
        cast(substr(a.region, 1, 255) as text(255)) as region,
        cast(substr(a.branch_office, 1, 255) as text(255)) as branch_office,
        cast(
            substr(a.business_unit_long_description, 1, 255) as text(255)
        ) as business_unit_long_description,
        cast(substr(a.department_type, 1, 255) as text(255)) as department_type,
        cast(
            substr(a.consolidated_shipment_dc_name, 1, 255) as text(255)
        ) as consolidated_shipment_dc_name,
        cast(substr(a.company_code, 1, 255) as text(255)) as company_code,
        cast(substr(a.company_name, 1, 255) as text(255)) as company_name,
        cast(substr(a.operating_company, 1, 255) as text(255))
            as operating_company,
        cast(substr(a.segment, 1, 255) as text(255)) as segment,
        cast(substr(a.active_cc_flag, 1, 255) as text(255)) as active_cc_flag,
        cast(substr(a.etl_exclude_flag, 1, 255) as text(255))
            as etl_exclude_flag,
        cast(substr(a.date_inserted, 1, 255) as text(255)) as date_inserted,
        cast(substr(a.date_updated, 1, 255) as text(255)) as date_updated,
        cast(substr(a.conv_status, 1, 255) as text(1)) as conv_status
    from final_conv as a
)

select * from dim_conv
