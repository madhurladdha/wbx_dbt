{{ config(tags=["manufacturing", "agreement", "wbx", "snapshot"]) }}
with
    prc_wtx_agreement_fact as (select * from {{ ref("fct_wbx_prc_agreement") }}),
    prc_wtx_po_fact as (
        select
            source_system,
            agreement_number as agreement_number,
            count(distinct po_order_number) po_order_count,
            sum(line_order_quantity) po_ordered_qty
        from {{ ref("fct_wbx_fin_prc_po") }}
        where line_status <> 'CANCELLED' and agreement_number <> '-'
        group by agreement_number, source_system
    ),
    prc_wtx_po_receipt_fact as (
        select
            source_system,
            agreement_number,
            sum(receipt_received_quantity) as receipt_received_quantity
        from {{ ref("fct_wbx_fin_prc_po_receipt") }}
        where line_status <> 'CANCELLED' and agreement_number <> '-'
        group by agreement_number, source_system
    ),
    source as (
        select
            '{{env_var("DBT_SOURCE_SYSTEM")}}' as source_system,
            to_date(convert_timezone('UTC', current_timestamp)) as snapshot_date,
            src.agreement_number,
            src.line_number,
            src.agreement_type_desc,
            src.source_company,
            src.source_business_unit_code,
            src.source_item_identifier,
            src.variant_code,
            src.site_code,
            src.status_code,
            src.status_desc,
            src.approval_status_code,
            src.approval_status_desc,
            src.agreement_eff_date,
            src.agreement_exp_date,
            src.supplier_address_number,
            src.agreement_quantity,
            src.original_quantity,
            src.price_per_unit,
            src.unit_of_measure,
            src.price_unit,
            src.currency_code,
            src.deleted_flag,
            src.item_guid,
            src.business_unit_address_guid,
            nvl(prc_wtx_po_fact.po_order_count, 0) as po_order_count,
            nvl(prc_wtx_po_fact.po_ordered_qty, 0) as po_ordered_qty,
            nvl(
                prc_wtx_po_receipt_fact.receipt_received_quantity, 0
            ) as receipt_received_quantity,
            src.source_updated_date,
            src.source_updated_time,
            nvl(src.released_quantity, 0) as released_quantity,
            nvl(src.received_quantity, 0) as received_quantity,
            nvl(src.invoiced_quantity, 0) as invoiced_quantity,
            nvl(src.remain_quantity, 0) as remain_quantity,
                        {{
                dbt_utils.surrogate_key(
                    [
                        "src.source_system",
                        "src.supplier_address_number",
                        "'SUPPLIER'",
                        "src.source_company",
                    ]
                )
            }} as supplier_address_number_guid
            
        from prc_wtx_agreement_fact src
        left outer join
           prc_wtx_po_fact
           on prc_wtx_po_fact.source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
           and src.agreement_number = prc_wtx_po_fact.agreement_number
        left outer join
          prc_wtx_po_receipt_fact
            on prc_wtx_po_receipt_fact.source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
            and src.agreement_number = prc_wtx_po_receipt_fact.agreement_number
    )

    select 
    cast(substring(source_system,1,255) as text(255) ) as source_system  ,

    cast(snapshot_date as date) as snapshot_date  ,

    cast(substring(agreement_number,1,255) as text(255) ) as agreement_number  ,

    cast(line_number as number(38,10) ) as line_number  ,

    cast(substring(agreement_type_desc,1,255) as text(255) ) as agreement_type_desc  ,

    cast(substring(source_company,1,255) as text(255) ) as source_company  ,

    cast(substring(source_business_unit_code,1,255) as text(255) ) as source_business_unit_code  ,

    cast(substring(source_item_identifier,1,255) as text(255) ) as source_item_identifier  ,

    cast(substring(variant_code,1,255) as text(255) ) as variant_code  ,

    cast(substring(site_code,1,255) as text(255) ) as site_code  ,

    cast(substring(status_code,1,255) as text(255) ) as status_code  ,

    cast(substring(status_desc,1,255) as text(255) ) as status_desc  ,

    cast(substring(approval_status_code,1,255) as text(255) ) as approval_status_code  ,

    cast(substring(approval_status_desc,1,255) as text(255) ) as approval_status_desc  ,

    cast(agreement_eff_date as date) as agreement_eff_date  ,

    cast(agreement_exp_date as date) as agreement_exp_date  ,

    cast(substring(supplier_address_number,1,255) as text(255) ) as supplier_address_number  ,

    cast(agreement_quantity as number(38,10) ) as agreement_quantity  ,

    cast(original_quantity as number(38,10) ) as original_quantity  ,

    cast(price_per_unit as number(38,10) ) as price_per_unit  ,

    cast(substring(unit_of_measure,1,255) as text(255) ) as unit_of_measure  ,

    cast(price_unit as number(38,10) ) as price_unit  ,

    cast(substring(currency_code,1,255) as text(255) ) as currency_code  ,

    cast(deleted_flag as number(38,0) ) as deleted_flag  ,

    cast(item_guid as text(255) ) as item_guid  ,

    cast(business_unit_address_guid as text(255) ) as business_unit_address_guid  ,

    cast(po_order_count as number(38,0) ) as po_order_count  ,

    cast(po_ordered_qty as number(38,0) ) as po_ordered_qty  ,

    cast(receipt_received_quantity as number(38,0) ) as receipt_received_qty  ,

    cast(source_updated_date as timestamp_ntz(6)) as source_updated_date  ,

    cast(source_updated_time as timestamp_ntz(6) ) as source_updated_time  ,

    cast(released_quantity as number(38,10) ) as released_quantity  ,

    cast(received_quantity as number(38,10) ) as received_quantity  ,

    cast(invoiced_quantity as number(38,10) ) as invoiced_quantity  ,

    cast(remain_quantity as number(38,10) ) as remain_quantity,
    cast(supplier_address_number_guid as text(255) ) as supplier_address_number_guid
 
    from source

