{{
    config(
        tags=["wbx", "manufacturing", "yield"],
        snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
    )
}}

with
    inv_wtx_stock_trans_fact as (
        select * from {{ ref("fct_wbx_mfg_inv_stock_trans") }}
    ),
    mfg_wtx_wo_produced_fact as (select * from {{ ref("fct_wbx_mfg_wo_produced") }}),
    mfg_wtx_mnthly_bom_snapshot as (
        select * from {{ ref("fct_wbx_mfg_monthly_bom_snapshot") }}
    ),
    dim_wbx_item as (select * from {{ ref("dim_wbx_item") }}),
    inv_wtx_stock_adj_fact as (select * from {{ ref("fct_wbx_mfg_inv_stock_adj") }}),
    stock_quantities as (
        select distinct
            x.source_bom_identifier,
            x.mnth_effective_date,
            x.mnth_expiration_date,
            x.comp_src_item_identifier,
            max(x.comp_item_uom) comp_item_uom,
            x.comp_src_variant_code,
            x.root_company_code,
            x.root_src_variant_code,
            sum(comp_required_qty) comp_required_qty,
            sum(comp_perfection_qty) comp_perfection_qty,
            max(comp_scrap_percent) comp_scrap_percent,
            max(root_qty) root_qty
        from mfg_wtx_mnthly_bom_snapshot x
        where x.bom_level = 1
        group by
            x.source_bom_identifier,
            x.mnth_effective_date,
            x.mnth_expiration_date,
            x.comp_src_item_identifier,
            x.comp_src_variant_code,
            x.root_company_code,
            x.root_src_variant_code
    ),
    item_master as (
        select distinct source_item_identifier, source_business_unit_code, item_type
        from dim_wbx_item
        where source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
    ),
    item_master1 as (
        select distinct source_item_identifier, max(item_type) item_type
        from dim_wbx_item
        where source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
        group by source_item_identifier
    ),
    bom_item as (
        select distinct
            s1.related_document_number,
            max(transaction_date) transaction_date,
            max(s1.stock_site) stock_site,
            max(s1.transaction_currency) transaction_currency,
            max(s1.voucher) voucher
        from inv_wtx_stock_trans_fact s1
        where voucher not like 'PP%'
        group by s1.related_document_number
    ),
    bom_quantities as (
        select distinct
            x.source_bom_identifier,
            x.mnth_effective_date,
            x.mnth_expiration_date,
            max(x.comp_item_uom) comp_item_uom,
            x.comp_src_item_identifier,
            x.comp_src_variant_code,
            x.root_company_code,
            x.root_src_variant_code,
            sum(comp_required_qty) comp_required_qty,
            sum(comp_perfection_qty) comp_perfection_qty,
            max(comp_scrap_percent) comp_scrap_percent,
            max(root_qty) root_qty,
            x.root_business_unit_code,
            max(x.item_model_group) item_model_group
        from mfg_wtx_mnthly_bom_snapshot x
        where x.bom_level = 1
        group by
            x.source_bom_identifier,
            x.mnth_effective_date,
            x.mnth_expiration_date,
            x.comp_src_item_identifier,
            x.comp_src_variant_code,
            x.root_company_code,
            x.root_src_variant_code,
            x.root_business_unit_code
    ),
    itm_master as (
        select distinct source_item_identifier, item_type
        from dim_wbx_item
        where source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
    ),
    source as (
        select
            'STOCK' as flag,
            s.related_document_number work_order_number,
            s.source_item_identifier comp_item_identifier,
            s.variant_code comp_variant_code,
            s.transaction_qty actual_transaction_qty,
            s.transaction_date as transaction_date,
            s.gl_date as gl_date,
            nvl(im.item_type, im1.item_type) comp_item_type,
            p.source_bom_identifier,
            p.source_item_identifier as wo_src_item_identifier,
            upper(p.source_business_unit_code) as source_business_unit_code,
            (
                case
                    when bi.root_qty <> 0
                    then (p.produced_qty * bi.comp_required_qty) / (bi.root_qty)
                    else 0
                end
            ) as comp_standard_qty,
            (
                case
                    when bi.root_qty <> 0
                    then (p.produced_qty * bi.comp_perfection_qty) / (bi.root_qty)
                    else 0
                end
            ) as comp_perfection_qty,
            bi.comp_scrap_percent,
            s.stock_site comp_stock_site,
            p.site financial_site,
            p.stock_site wo_stock_site,
            s.voucher,
            upper(p.company_code) company_code,
            s.transaction_uom,
            s.transaction_currency,
            bi.root_src_variant_code wo_src_variant_code,
            (
                case when bi.comp_src_item_identifier is null then 'N' else 'Y' end
            ) as item_match_bom_flag,
            s.transaction_amt,
            0 as stock_adj_qty,
            s.product_class,
            s.item_model_group comp_item_model_group,
            p.item_model_group wo_item_model_group,
            p.bulk_flag,
            p.consolidated_batch_order
        from inv_wtx_stock_trans_fact s
        -- Produced Fact to get BOM IDENTIFIER
        inner join
            mfg_wtx_wo_produced_fact p
            on s.related_document_number = p.work_order_number
            and p.status_code = 7
        -- Join to EBOM to get Perfection, Std Qty
        left outer join
            stock_quantities bi
            on p.source_bom_identifier = bi.source_bom_identifier
            and upper(p.company_code) = upper(bi.root_company_code)
            and s.transaction_date
            between bi.mnth_effective_date and bi.mnth_expiration_date  -- bi.snapshot_date =current_date
            and bi.comp_src_item_identifier = s.source_item_identifier
            and s.variant_code = bi.comp_src_variant_code
        -- Join to Item master with business unit and without to get component item
        -- type
        left join
            item_master im
            on s.source_item_identifier = im.source_item_identifier
            and s.source_business_unit_code = im.source_business_unit_code
        inner join
            item_master1 im1 on s.source_item_identifier = im1.source_item_identifier
        where
            s.voucher not like 'PP%'
            and (
                upper(s.status_desc) = 'SOLD'
                or (upper(s.status_desc) = 'PURCHASED' and invoice_returned_flag = 1)
            )

        union all

        -- Entries for Components in BOM that are not in Stock transactions
        /* 2nd part - BOM items that do not exist in stock trans for a BOM item*/
        select distinct
            'BOM' as flag,
            p.work_order_number work_order_number,
            bi.comp_src_item_identifier comp_item_identifier,
            bi.comp_src_variant_code comp_variant_code,
            0 actual_transaction_qty,
            s.transaction_date transaction_date,
            s.transaction_date as gl_date,
            nvl(im.item_type, im1.item_type) comp_item_type,
            p.source_bom_identifier,
            p.source_item_identifier as wo_src_item_identifier,
            upper(p.source_business_unit_code) as source_business_unit_code,
            (
                case
                    when bi.root_qty <> 0
                    then (p.produced_qty * bi.comp_required_qty) / (bi.root_qty)
                    else 0
                end
            ) as comp_standard_qty,
            (
                case
                    when bi.root_qty <> 0
                    then (p.produced_qty * bi.comp_perfection_qty) / (bi.root_qty)
                    else 0
                end
            ) as comp_perfection_qty,
            bi.comp_scrap_percent,
            s.stock_site comp_stock_site,
            p.site financial_site,
            p.stock_site wo_stock_site,
            s.voucher,
            upper(p.company_code) company_code,
            bi.comp_item_uom transaction_uom,
            s.transaction_currency,
            bi.root_src_variant_code wo_src_variant_code,
            'N' as item_match_bom_flag,
            0 as transaction_amt,
            0 as stock_adj_qty,
            p.product_class,
            bi.item_model_group comp_item_model_group,
            p.item_model_group wo_item_model_group,
            p.bulk_flag,
            p.consolidated_batch_order
        from bom_item s
        -- Produced Fact to get BOM IDENTIFIER
        inner join
            mfg_wtx_wo_produced_fact p
            on s.related_document_number = p.work_order_number
            and p.status_code = 7
        -- Join to EBOM to get Perfection, Std Qty
        inner join
            bom_quantities bi
            on p.source_bom_identifier = bi.source_bom_identifier
            and upper(p.company_code) = upper(bi.root_company_code)
            -- and bi.snapshot_date =current_date
            and p.actual_completion_date
            between bi.mnth_effective_date and bi.mnth_expiration_date
        -- Join to Item master with business unit and without to get component item
        -- type
        left join
            item_master im
            on bi.comp_src_item_identifier = im.source_item_identifier
            and bi.root_business_unit_code = im.source_business_unit_code
        inner join
            item_master1 im1 on bi.comp_src_item_identifier = im1.source_item_identifier
        where
            (
                bi.comp_src_item_identifier
                || '-'
                || bi.comp_src_variant_code
                || '-'
                || s.related_document_number
            )
            != all
            -- Join to eliminate entries covered in the earlier union
            (
                select distinct
                    source_item_identifier
                    || '-'
                    || variant_code
                    || '-'
                    || related_document_number
                from inv_wtx_stock_trans_fact
                where
                    voucher not like 'PP%'
                    and (
                        upper(status_desc) = 'SOLD'
                        or (
                            upper(status_desc) = 'PURCHASED'
                            and invoice_returned_flag = 1
                        )
                    )
            )

        union all
        -- Stock Adjustments                                  
        select
            'ADJUSTMENT' as flag,
            null as work_order_number,
            s.source_item_identifier comp_item_identifier,
            s.variant_code comp_variant_code,
            0 actual_transaction_qty,
            s.transaction_date as transaction_date,
            s.gl_date as gl_date,
            im.item_type comp_item_type,
            null as source_bom_identifier,
            null as wo_src_item_identifier,
            s.source_business_unit_code as source_business_unit_code,
            0 as comp_standard_qty,
            0 as comp_perfection_qty,
            0 as comp_scrap_percent,
            s.stock_site comp_stock_site,
            s.site financial_site,
            null wo_stock_site,
            s.voucher,
            upper(s.company_code) company_code,
            s.transaction_uom,
            s.transaction_currency,
            null wo_src_variant_code,
            'N' as item_match_bom_flag,
            s.transaction_amt,
            s.transaction_qty stock_adj_qty,
            s.product_class,
            null as comp_item_model_group,
            null as wo_item_model_group,
            null as bulk_flag,
            null as consolidated_batch_order
        from inv_wtx_stock_adj_fact s
        inner join
            itm_master im
            on s.source_item_identifier = im.source_item_identifier
            and im.item_type in ('INGREDIENT', 'PACKAGING')
            and s.source_account_identifier = '521020'
    )

select *
from source
