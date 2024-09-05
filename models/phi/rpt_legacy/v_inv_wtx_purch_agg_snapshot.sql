{{ config(materialized=env_var("DBT_MAT_VIEW"), tags=["manufacturing","agreement","wbx"]) }}

with cte_prc_wtx_agrmnt_snapshot_fact as (
    select * from {{ ref('fct_wbx_prc_agreement_balance_snapshot') }}
),

cte_item as (select * from {{ ref("dim_wbx_item") }}),

cte_plant_dc as (select * from {{ ref('dim_wbx_plant_dc') }}),

cte_supplier as (select * from {{ ref("dim_wbx_supplier") }}),

cte_time_variant_dim as (select * from {{ ref("dim_wbx_mfg_item_variant") }}),

cte_final as (
    select
        a.source_system,

        snapshot_date,

        agreement_number,

        line_number,

        agreement_type_desc,

        source_company,

        a.source_business_unit_code,

        a.source_item_identifier,

        a.variant_code,

        site_code,

        status_code,

        status_desc,

        approval_status_code,

        approval_status_desc,

        agreement_eff_date,

        agreement_exp_date,

        a.supplier_address_number,

        agreement_quantity,

        original_quantity,

        price_per_unit,

        unit_of_measure,

        price_unit,

        a.currency_code,

        deleted_flag,

        a.item_guid,

        a.business_unit_address_guid,

        po_order_count,

        po_ordered_qty,

        receipt_received_qty,

        source_updated_date,

        source_updated_time,

        (case

            when (wd.variant_desc is NULL or TRIM(wd.variant_desc) = '')

                then

                    TO_CHAR(d.description)

            else

                TO_CHAR(wd.variant_desc)

        end) as description,

        d.buyer_code,

        d.vendor_address_guid,

        e.supplier_name,

        a.released_quantity,

        a.received_quantity,

        a.invoiced_quantity,

        a.remain_quantity

    from cte_prc_wtx_agrmnt_snapshot_fact as a

    left join

        (select distinct
            source_item_identifier,

            source_system,

            MAX(primary_uom) as primary_uom,

            MAX(item_type) as item_type,

            MAX(description) as description,

            MAX(buyer_code) as buyer_code,

            MAX(vendor_address_guid) as vendor_address_guid

        from cte_item

        where source_system = '{{ env_var('DBT_SOURCE_SYSTEM') }}'

        group by source_item_identifier, source_system) as d /*,DESCRIPTION*/

        on
            a.source_item_identifier = d.source_item_identifier

            and a.source_system = d.source_system

    left join cte_plant_dc as b --Amy changed to Left join Jan 16 2019 to allow agreements without a location to come thru

        on
            a.source_business_unit_code =

            b.source_business_unit_code

            and a.source_system = b.source_system

    left join cte_supplier as e

        on
            e.source_system = a.source_system

            and case

                when a.source_system = '{{ env_var('DBT_SOURCE_SYSTEM') }}'

                    then

                        e.source_system_address_number
            end = a.supplier_address_number
            and UPPER(TRIM(e.company_code)) = UPPER(TRIM(a.source_company))
    left join

        (
            select
                s.source_system,

                s.source_item_identifier,

                TRIM(s.variant_code) as variant_code,

                s.active_flag,

                MAX(s.variant_desc) as variant_desc,

                MAX(s.item_allocation_key) as item_allocation_key

            from cte_time_variant_dim as s,

                (
                    select
                        source_system,

                        source_item_identifier,

                        TRIM(variant_code) as variant_code,

                        MAX(active_flag) as active_flag

                    from cte_time_variant_dim

                    group by
                        source_system,

                        source_item_identifier,

                        TRIM(variant_code)
                ) as d

            where
                s.source_item_identifier = d.source_item_identifier

                and TRIM(s.variant_code) = TRIM(d.variant_code)

                and s.source_system = d.source_system

                and s.active_flag = d.active_flag

            group by
                s.source_system,

                s.source_item_identifier,

                TRIM(s.variant_code),

                s.active_flag
        ) as wd

        on
            a.source_system = wd.source_system

            and a.source_item_identifier = wd.source_item_identifier

            and TRIM(a.variant_code) = TRIM(wd.variant_code)
)

select
    source_system,
    snapshot_date,
    agreement_number,
    line_number,
    agreement_type_desc,
    source_company,
    source_business_unit_code,
    source_item_identifier,
    variant_code,
    site_code,
    status_code,
    status_desc,
    approval_status_code,
    approval_status_desc,
    agreement_eff_date,
    agreement_exp_date,
    supplier_address_number,
    agreement_quantity,
    original_quantity,
    price_per_unit,
    unit_of_measure,
    price_unit,
    currency_code,
    deleted_flag,
    item_guid,
    business_unit_address_guid,
    po_order_count,
    po_ordered_qty,
    receipt_received_qty,
    source_updated_date,
    source_updated_time,
    description,
    buyer_code,
    vendor_address_guid,
    supplier_name,
    released_quantity,
    received_quantity,
    invoiced_quantity,
    remain_quantity
from cte_final