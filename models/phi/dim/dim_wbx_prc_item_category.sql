{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        transient=false,
        unique_key="UNIQUE_KEY",
        on_schema_change="sync_all_columns",
        full_refresh=false,
    )
}}


with
    item as (select * from {{ ref("int_d_wbx_item") }}),

    conv_item as (select * from {{ ref("conv_dim_wbx_prc_item_category") }}),

    item_proc as (
        select
            {{
                dbt_utils.surrogate_key(
                    ["item.item_guid", "item.business_unit_address_guid"]
                )
            }} as unique_key,
            source_system,
            source_item_identifier,
            item_guid_old as item_guid_old,
            item_guid as item_guid,
            source_business_unit_code,
            business_unit_address_guid_old,
            business_unit_address_guid,
            /* fields for Procurement Category */
            highlevel_category_code as highlevel_category_code,
            midlevel_category_code as midlevel_category_code,
            lowlevel_category_code as lowlevel_category_code,
            master_reporting_category as master_reporting_category,
            alternate_reporting_category as alternate_reporting_category,
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
            buyer_name as buyer_name,
            lead_time as lead_time,
            safety_stock,
            master_planning_family_code,
            systimestamp() as update_date,
            systimestamp() as load_date
        from item

    ),

    new_dim as (
        select
            a.unique_key,
            a.source_system,
            a.source_item_identifier,
            b.item_guid_old,
            a.item_guid,
            a.source_business_unit_code,
            b.business_unit_address_guid_old,
            a.business_unit_address_guid,
            a.highlevel_category_code,
            a.midlevel_category_code,
            a.lowlevel_category_code,
            a.lead_time,
            a.master_reporting_category,
            a.alternate_reporting_category,
            a.update_date,
            a.load_date,
            a.buyer_name,
            a.safety_stock,
            a.master_planning_family_code,
            null as packaging_die_size_code,
            a.item_category_1,
            a.item_category_2,
            a.item_category_3,
            a.item_category_4,
            a.item_category_5,
            a.item_category_6,
            a.item_category_7,
            a.item_category_8,
            a.item_category_9,
            a.item_category_10
        from item_proc a
        left outer join conv_item b on a.unique_key = b.unique_key
    ),

    old_dim as (
        select
            a.unique_key,
            a.source_system,
            a.source_item_identifier,
            a.item_guid_old,
            a.item_guid,
            a.source_business_unit_code,
            a.business_unit_address_guid_old,
            a.business_unit_address_guid,
            a.highlevel_category_code,
            a.midlevel_category_code,
            a.lowlevel_category_code,
            a.lead_time,
            a.master_reporting_category,
            a.alternate_reporting_category,
            a.update_date,
            a.load_date,
            a.buyer_name,
            a.safety_stock,
            a.master_planning_family_code,
            null as packaging_die_size_code,
            a.item_category_1,
            a.item_category_2,
            a.item_category_3,
            a.item_category_4,
            a.item_category_5,
            a.item_category_6,
            a.item_category_7,
            a.item_category_8,
            a.item_category_9,
            a.item_category_10
        from conv_item a
        left outer join item_proc b on a.unique_key = b.unique_key
        where b.unique_key is null
    ),

    final_dim as (

        select * from new_dim
        union
        select * from old_dim
    ),

    item_proc_with_cast as (
        select
    cast(substr(unique_key, 1, 255) AS varchar2(255)) AS unique_key
	,cast(substr(source_system, 1, 255) AS varchar2(255)) AS source_system
	,cast(substr(source_item_identifier, 1, 60) AS varchar2(60)) AS source_item_identifier
	,cast(item_guid_old AS varchar2(255)) AS item_guid_old
	,cast(substr(item_guid, 1, 255) AS varchar2(255)) AS item_guid
	,cast(substr(source_business_unit_code, 1, 10) AS varchar2(10)) AS source_business_unit_code
	,cast(business_unit_address_guid_old AS varchar2(255)) AS business_unit_address_guid_old
	,cast(substr(business_unit_address_guid, 1, 255) AS varchar2(255)) AS business_unit_address_guid
	,cast(substr(highlevel_category_code, 1, 255) AS varchar2(255)) AS highlevel_category_code
	,cast(substr(midlevel_category_code, 1, 255) AS varchar2(255)) AS midlevel_category_code
	,cast(substr(lowlevel_category_code, 1, 255) AS varchar2(255)) AS lowlevel_category_code
	,cast(lead_time as numeric(38, 10)) AS lead_time
	,cast(substr(master_reporting_category, 1, 255) AS varchar2(255)) AS master_reporting_category
	,cast(substr(alternate_reporting_category, 1, 255) AS varchar2(255)) AS alternate_reporting_category
	,cast(update_date AS timestamp_ntz(9)) AS update_date
	,cast(load_date as timestamp_ntz(9)) AS load_date
	,cast(substr(buyer_name, 1, 255) AS varchar2(255)) AS buyer_name
	,cast(safety_stock AS number(38, 10)) AS safety_stock
	,cast(substring(master_planning_family_code, 1, 60) AS TEXT (60)) AS master_planning_family_code
	,cast(substring(packaging_die_size_code, 1, 60) AS TEXT (60)) AS packaging_die_size_code
	,cast(substring(item_category_1, 1, 255) AS varchar2(255)) AS item_category_1
	,cast(substring(item_category_2, 1, 255) AS varchar2(255)) AS item_category_2
	,cast(substring(item_category_3, 1, 255) AS varchar2(255)) AS item_category_3
	,cast(substring(item_category_4, 1, 255) AS varchar2(255)) AS item_category_4
	,cast(substring(item_category_5, 1, 255) AS varchar2(255)) AS item_category_5
	,cast(substring(item_category_6, 1, 255) AS varchar2(255)) AS item_category_6
	,cast(substring(item_category_7, 1, 255) AS varchar2(255)) AS item_category_7
	,cast(substring(item_category_8, 1, 255) AS varchar2(255)) AS item_category_8
	,cast(substring(item_category_9, 1, 255) AS varchar2(255)) AS item_category_9
	,cast(substring(item_category_10, 1, 255) AS varchar2(255)) AS item_category_10

        from final_dim
    )

select * from item_proc_with_cast
