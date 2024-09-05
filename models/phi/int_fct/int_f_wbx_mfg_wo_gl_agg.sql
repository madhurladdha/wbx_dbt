{{ config(tags=["wbx", "manufacturing", "work_order", "gl", "agg"]) }}

/* For D365 BR1 (and BR2), this model is changed around the join w/ the CBOM model.
    The entire model here needs to have company code (or document company) as part of the higher level keys and joins between the 
    various tables, including CBOM.  The changes here are to pull the company from each and add them to any group-by statements
    and for the joins between the tables.

    This is to ensure that the 3 companies (WBX, RFL, and IBE) are handled separately and correctly.
    IBE may not be applicable for the final output data for Mfg, Work Orders, Yield, etc, but these models should be separated by that 
    value.
*/

with
    mfg_wtx_wo_gl_fact as (select * from {{ ref("fct_wbx_mfg_wo_gl") }}),
    mfg_wtx_yield_agg_fact as (select * from {{ ref('fct_wbx_mfg_yield_agg') }}),
    mfg_wtx_wo_produced_fact as (select * from {{ ref("fct_wbx_mfg_wo_produced") }}),
    mfg_wtx_cbom_fact as (select * from {{ ref("fct_wbx_mfg_cbom") }}),
    gl_fact as (
        -- GL Fact all vouchers with WO
        select
            f.source_system,
            f.document_company,
            f.voucher voucher,
            f.reference_id work_order_number,
            max(f.journal_number) journal_number,
            max(f.source_site_code) source_site_code,
            max(f.gl_date) gl_date,
            max(f.recipecalc_date) as recipecalc_date,
            f.transaction_currency,
            sum(
                case
                    when source_account_identifier = '550010'
                    then f.transaction_amount
                    else 0
                end
            ) as a550010_gl_amount,
            sum(
                case
                    when source_account_identifier = '550015'
                    then f.transaction_amount
                    else 0
                end
            ) as a550015_gl_amount,
            sum(
                case
                    when source_account_identifier = '510045'
                    then f.transaction_amount
                    else 0
                end
            ) as a510045_gl_amount,
            sum(
                case
                    when source_account_identifier = '718020'
                    then f.transaction_amount
                    else 0
                end
            ) as a718020_gl_amount,
            sum(
                case
                    when source_account_identifier = '718040'
                    then f.transaction_amount
                    else 0
                end
            ) as a718040_gl_amount,
            sum(
                case
                    when source_account_identifier = '718060'
                    then f.transaction_amount
                    else 0
                end
            ) as a718060_gl_amount,
            sum(
                case
                    when source_account_identifier = '550030'
                    then f.transaction_amount
                    else 0
                end
            ) as a550030_gl_amount,
            max(source_business_unit_code) source_business_unit_code
        from mfg_wtx_wo_gl_fact f
        where
            f.source_account_identifier
            in ('550010', '550015', '510045', '718020', '718040', '718060', '550030')
        group by
            f.source_system,
            f.document_company,
            f.reference_id,
            f.voucher,
            f.transaction_currency
        having not (reference_id is null or reference_id = '')
        union
        -- GL Fact all vouchers without WO
        select
            f.source_system,
            f.document_company,
            f.voucher voucher,
            f.reference_id work_order_number,
            journal_number,
            max(f.source_site_code) source_site_code,
            max(f.gl_date) gl_date,
            max(f.recipecalc_date) as recipecalc_date,
            f.transaction_currency,
            sum(
                case
                    when source_account_identifier = '550010'
                    then f.transaction_amount
                    else 0
                end
            ) as a550010_gl_amount,
            sum(
                case
                    when source_account_identifier = '550015'
                    then f.transaction_amount
                    else 0
                end
            ) as a550015_gl_amount,
            sum(
                case
                    when source_account_identifier = '510045'
                    then f.transaction_amount
                    else 0
                end
            ) as a510045_gl_amount,
            sum(
                case
                    when source_account_identifier = '718020'
                    then f.transaction_amount
                    else 0
                end
            ) as a718020_gl_amount,
            sum(
                case
                    when source_account_identifier = '718040'
                    then f.transaction_amount
                    else 0
                end
            ) as a718040_gl_amount,
            sum(
                case
                    when source_account_identifier = '718060'
                    then f.transaction_amount
                    else 0
                end
            ) as a718060_gl_amount,
            sum(
                case
                    when source_account_identifier = '550030'
                    then f.transaction_amount
                    else 0
                end
            ) as a550030_gl_amount,
            max(source_business_unit_code) source_business_unit_code
        from mfg_wtx_wo_gl_fact f
        where
            f.source_account_identifier
            in ('550010', '550015', '510045', '718020', '718040', '718060', '550030')
        group by
            f.source_system,
            f.document_company,
            f.reference_id,
            f.voucher,
            f.transaction_currency,
            journal_number
        having (reference_id is null or reference_id = '')
        union
        select
            source_system,
            company_code document_company,
            voucher,
            work_order_number,
            null as journal_number,
            max(wo_stock_site) source_site_code,
            max(transaction_date) gl_date,
            max(gl_date) as recipecalc_date,
            transaction_currency,
            0 as a550010_gl_amount,
            0 as a550015_gl_amount,
            0 as a510045_gl_amount,
            0 as a718020_gl_amount,
            0 as a718040_gl_amount,
            0 as a718060_gl_amount,
            0 as a550030_gl_amount,
            max(source_business_unit_code) source_business_unit_code
        from mfg_wtx_yield_agg_fact
        where
            work_order_number || '-' || voucher not in (
                select reference_id || '-' || voucher
                from mfg_wtx_wo_gl_fact f
                where
                    f.source_account_identifier in (
                        '550010',
                        '550015',
                        '510045',
                        '718020',
                        '718040',
                        '718060',
                        '550030'
                    )
            )
        group by
            source_system,
            company_code,
            work_order_number,
            transaction_currency,
            voucher
    ),
    actual_amount as (
        select
            company_code,
            work_order_number,
            voucher,
            wo_src_item_identifier,
            wo_src_variant_code,
            max(wo_stock_site) wo_stock_site,
            max(transaction_date) transaction_date,
            sum(
                case
                    when upper(comp_item_model_group) = 'SERVICE'
                    then transaction_amt
                    else 0
                end
            ) service_transaction_amt,
            sum(
                case
                    when upper(comp_item_model_group) <> 'SERVICE'
                    then gldt_actual_amount
                    else 0
                end
            ) actual_transaction_amt,
            sum(perfection_amount) perfection_amt,
            sum(standard_amount) standard_amt,
            max(wo_item_model_group) item_model_group
        from mfg_wtx_yield_agg_fact
        group by company_code, work_order_number, voucher, wo_src_item_identifier, wo_src_variant_code
    ),
    produced_quantity as (
        select company_code, work_order_number, produced_qty, bulk_flag from mfg_wtx_wo_produced_fact
    ),
    cbom_w_variant as (
        select
            root_company_code,
            root_src_item_identifier,
            root_src_variant_code,
            stock_site,
            eff_date,
            expir_date,
            sum(b.comp_item_unit_cost) receipe_value
        from mfg_wtx_cbom_fact b
        where  -- NOT ((b.COMP_BOM_LEVEL = 0) AND b.PARENT_ITEM_INDICATOR = 'Item') and --drop items that are top level of BOM, but are not parents            
            (
                (
                    b.comp_bom_level = 1  -- Required for Yield
                    and (
                        b.comp_calctype_desc in ('Item', 'Service')
                        or (
                            b.comp_calctype_desc in ('BOM')
                            and b.parent_item_indicator in ('Item', 'Parent')
                        )
                    )
                )
                or (
                    b.comp_bom_level = 0
                    and b.comp_calctype_desc in ('Production')
                    and b.parent_item_indicator = 'Item'
                )
            )
        -- AND (b.COMP_CALCTYPE_DESC IN('Item','Production') OR
        -- (b.COMP_CALCTYPE_DESC IN('BOM') AND b.PARENT_ITEM_INDICATOR = 'Item')
        group by
            root_company_code,
            root_src_item_identifier,
            root_src_variant_code,
            stock_site,
            eff_date,
            expir_date
    ),
    cbom_wo_variant as (
        select
            root_company_code,
            root_src_item_identifier,
            root_src_variant_code,
            stock_site,
            eff_date,
            expir_date,
            sum(b1.comp_item_unit_cost) receipe_value
        from mfg_wtx_cbom_fact b1
        where  -- NOT ((b1.COMP_BOM_LEVEL = 0) AND b1.PARENT_ITEM_INDICATOR = 'Item') and --drop items that are top level of BOM, but are not parents       
            (
                (
                    b1.comp_bom_level = 1  -- Required for Yield 
                    and (
                        b1.comp_calctype_desc in ('Item', 'Service')
                        or (
                            b1.comp_calctype_desc in ('BOM')
                            and b1.parent_item_indicator in ('Item', 'Parent')
                        )
                    )
                )
                or (
                    b1.comp_bom_level = 0
                    and b1.comp_calctype_desc in ('Production')
                    and b1.parent_item_indicator = 'Item'
                )
            )
        -- and (b1.ROOT_SRC_VARIANT_CODE is null or b1.ROOT_SRC_VARIANT_CODE = '')
        group by
            root_company_code,
            root_src_item_identifier,
            root_src_variant_code,
            stock_site,
            eff_date,
            expir_date
    ),
    final as (
        select
            f.source_system,
            f.document_company,
            f.voucher,
            f.work_order_number,
            f.journal_number,
            a.wo_stock_site wo_stock_site,
            f.gl_date,
            f.transaction_currency,
            a.wo_src_item_identifier,
            a.wo_src_variant_code,
            a.item_model_group,
            a550010_gl_amount a550010_gl_amount,
            a550015_gl_amount a550015_gl_amount,
            a510045_gl_amount a510045_gl_amount,
            a718020_gl_amount a718020_gl_amount,
            a718040_gl_amount a718040_gl_amount,
            a718060_gl_amount a718060_gl_amount,
            a550030_gl_amount,
            nvl(
                coalesce(b.receipe_value, b1.receipe_value) * d.produced_qty, 0
            ) receipe_value,
            d.bulk_flag as bulk_order_flag,
            nvl(a.actual_transaction_amt, 0) actual_transaction_amt,
            nvl(a.service_transaction_amt, 0) as service_transaction_amt,
            nvl(a.perfection_amt, 0) as perfection_amt,
            nvl(a.standard_amt, 0) as standard_amt,
            nvl(produced_qty, 0) as produced_qty,
            f.source_business_unit_code,
            current_timestamp as load_date,
            current_timestamp as update_date
        from gl_fact f
        left outer join
            actual_amount a
            on f.document_company = a.company_code
            and f.work_order_number = a.work_order_number
            and f.voucher = a.voucher
        left outer join produced_quantity d 
            on a.company_code = d.company_code
            and a.work_order_number = d.work_order_number
        left outer join
            cbom_w_variant b  -- Required for Yield
            on b.root_company_code = a.company_code
            and b.root_src_item_identifier = a.wo_src_item_identifier
            and b.root_src_variant_code = a.wo_src_variant_code
            and b.stock_site = a.wo_stock_site
            and f.recipecalc_date between b.eff_date and b.expir_date
        left outer join
            cbom_wo_variant b1  -- Required for Yield
            on b.root_company_code = a.company_code
            and b1.root_src_item_identifier = a.wo_src_item_identifier
            and b1.stock_site = a.wo_stock_site
            and f.recipecalc_date between b1.eff_date and b1.expir_date
    )

select
    *,
    {{
        dbt_utils.surrogate_key(
            [
                "source_system",
                "document_company",
                "voucher",
                "work_order_number",
                "wo_stock_site",
                "transaction_currency",
            ]
        )
    }} as unique_key
from final
