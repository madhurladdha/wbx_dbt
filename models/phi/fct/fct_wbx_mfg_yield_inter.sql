{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        tags=["wbx", "manufacturing", "yield"],
        snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
        pre_hook="""
                 {{ truncate_if_exists(this.schema, this.table) }}
                 """,
    )
}}

/* For D365 BR1 (and BR2), this model is changed around the join w/ the CBOM model.
    The entire model here needs to have company code (or document company) as part of the higher level keys and joins between the 
    various tables, including CBOM.  The changes here are to pull the company from each and add them to any group-by statements
    and for the joins between the tables.

    This is to ensure that the 3 companies (WBX, RFL, and IBE) are handled separately and correctly.
    IBE may not be applicable for the final output data for Mfg, Work Orders, Yield, etc, but these models should be separated by that 
    value.
*/

with

    source as (select * from {{ ref("int_f_wbx_mfg_yield_inter") }}),
    fct_wbx_mfg_cbom as (
        select * from {{ ref("fct_wbx_mfg_cbom") }} where comp_bom_level = 0
    ),
    -- duplicate values coming due to different eff_date and expir_date,
    -- to handle pick any row as in IICS the below code has been implemented
    fct_wbx_mfg_cbom_wo_var2 as (
        select *
        from
            (
                select
                    *,
                    row_number() over (
                        partition by
                            root_company_code, root_src_item_identifier, stock_site, transaction_date
                        order by expir_date desc
                    ) rnk
                from
                    (
                        select distinct
                            fctwovar2.root_company_code,
                            fctwovar2.root_src_item_identifier,
                            fctwovar2.stock_site,
                            fctwovar2.eff_date,
                            fctwovar2.expir_date,
                            fctwovar2.root_src_unit_price,
                            src.transaction_date
                        from source src
                        left join
                            fct_wbx_mfg_cbom fctwovar2
                            on fctwovar2.root_company_code = src.company_code
                            and fctwovar2.root_src_item_identifier = src.comp_item_identifier
                            and fctwovar2.stock_site = src.comp_stock_site
                            and fctwovar2.eff_date <= src.transaction_date
                            and fctwovar2.expir_date >= src.transaction_date
                    )
            )
        where rnk = 1
    ),
    -- duplicate values coming due to different eff_date and expir_date,
    -- to handle pick any row as in IICS the below code has been implemented
    fct_wbx_mfg_cbom_wo_var1 as (
        select *
        from
            (
                select
                    *,
                    row_number() over (
                        partition by root_company_code, root_src_item_identifier, stock_site, gl_date
                        order by expir_date desc
                    ) rnk
                from
                    (
                        select distinct
                            fctwovar1.root_company_code,
                            fctwovar1.root_src_item_identifier,
                            fctwovar1.stock_site,
                            fctwovar1.eff_date,
                            fctwovar1.expir_date,
                            fctwovar1.root_src_unit_price,
                            src.gl_date
                        from source src
                        left join
                            fct_wbx_mfg_cbom fctwovar1
                            on fctwovar1.root_company_code = src.company_code
                            and fctwovar1.root_src_item_identifier = src.comp_item_identifier
                            and fctwovar1.stock_site = src.comp_stock_site
                            and fctwovar1.eff_date <= src.gl_date
                            and fctwovar1.expir_date >= src.gl_date
                    )
            )
        where rnk = 1
    ),

    final as (
        select
            src.*,
            '{{env_var("DBT_SOURCE_SYSTEM")}}' as source_system,
            current_timestamp as load_date,
            current_timestamp as update_date,
            null as gldt_stock_adj_amount,

            nvl(
                case
                    when fct1.root_src_unit_price is null
                    then fctwovar1.root_src_unit_price
                    else fct1.root_src_unit_price
                end,
                0
            ) as gldt_unit_price,
            nvl(
                case
                    when fct2.root_src_unit_price is null
                    then fctwovar2.root_src_unit_price
                    else fct2.root_src_unit_price
                end,
                0
            ) as trandt_unit_price,
            nvl(
                case
                    when fct1.root_src_unit_price is null
                    then fctwovar1.root_src_unit_price * src.actual_transaction_qty
                    else fct1.root_src_unit_price * src.actual_transaction_qty
                end,
                0
            ) as gldt_actual_amount,
            nvl(
                case
                    when fct2.root_src_unit_price is null
                    then fctwovar2.root_src_unit_price * src.actual_transaction_qty
                    else fct2.root_src_unit_price * src.actual_transaction_qty
                end,
                0
            ) as trandt_actual_amount,
            nvl(
                case
                    when fct2.root_src_unit_price is null
                    then fctwovar2.root_src_unit_price * src.comp_standard_qty
                    else fct2.root_src_unit_price * src.comp_standard_qty
                end,
                0
            ) as standard_amount,
            nvl(
                case
                    when fct2.root_src_unit_price is null
                    then fctwovar2.root_src_unit_price * src.comp_perfection_qty
                    else fct2.root_src_unit_price * src.comp_perfection_qty
                end,
                0
            ) as perfection_amount

        from source src

        left join
            fct_wbx_mfg_cbom fct1
            on fct1.root_company_code = src.company_code
            and fct1.root_src_item_identifier = src.comp_item_identifier
            and fct1.root_src_variant_code = src.comp_variant_code
            and fct1.stock_site = src.comp_stock_site
            and fct1.eff_date <= src.gl_date
            and fct1.expir_date >= src.gl_date
        left join
            fct_wbx_mfg_cbom fct2
            on fct2.root_company_code = src.company_code
            and fct2.root_src_item_identifier = src.comp_item_identifier
            and fct2.root_src_variant_code = src.comp_variant_code
            and fct2.stock_site = src.comp_stock_site
            and fct2.eff_date <= src.transaction_date
            and fct2.expir_date >= src.transaction_date
        left join
            fct_wbx_mfg_cbom_wo_var1 fctwovar1
            on fctwovar1.root_company_code = src.company_code
            and fctwovar1.root_src_item_identifier = src.comp_item_identifier
            and fctwovar1.stock_site = src.comp_stock_site
            and fctwovar1.gl_date = src.gl_date

        left join
            fct_wbx_mfg_cbom_wo_var2 fctwovar2
            on fctwovar2.root_company_code = src.company_code
            and fctwovar2.root_src_item_identifier = src.comp_item_identifier
            and fctwovar2.stock_site = src.comp_stock_site
            and fctwovar2.transaction_date = src.transaction_date

    )

select
    source_system,
    comp_stock_site,
    financial_site,
    voucher,
    work_order_number,
    comp_item_identifier as comp_src_item_identifier,
    comp_variant_code as comp_src_variant_code,
    transaction_date,
    comp_item_type,
    source_bom_identifier,
    wo_src_item_identifier,
    source_business_unit_code,
    company_code,
    transaction_uom as comp_transaction_uom,
    transaction_currency,
    wo_src_variant_code,
    actual_transaction_qty,
    comp_standard_qty as comp_standard_quantity,
    comp_perfection_qty as comp_perfection_quantity,
    comp_scrap_percent,
    item_match_bom_flag,
    transaction_amt,
    stock_adj_qty,
    product_class,
    consolidated_batch_order,
    bulk_flag,
    gl_date,
    gldt_unit_price,
    trandt_unit_price,
    gldt_actual_amount,
    trandt_actual_amount,
    standard_amount,
    gldt_stock_adj_amount,
    perfection_amount,
    load_date,
    comp_item_model_group,
    wo_item_model_group,
    wo_stock_site,
    update_date,
    flag
from final
