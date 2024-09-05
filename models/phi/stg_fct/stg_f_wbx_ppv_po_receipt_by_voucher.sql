/*

this is model is equivalent to the view POSTSNOWP.R_EI_SYSADM.V_PPV_WTX_PO_RECEIPT_BY_VOUCHER
which is being used in the map m_PRC_WTX_PPV_FACT

After doing minus dbt and iics, 466 records, 

select count(*) from stg_f_wbx_ppv_po_receipt_by_voucher; --102902 records in total

select count(*) from POSTSNOWP.R_EI_SYSADM.V_PPV_WTX_PO_RECEIPT_BY_VOUCHER;

select * from POSTSNOWP.R_EI_SYSADM.V_PPV_WTX_PO_RECEIPT_BY_VOUCHER --466 
minus 
select * from stg_f_wbx_ppv_po_receipt_by_voucher;

select * from stg_f_wbx_ppv_po_receipt_by_voucher --466
minus 
select * from POSTSNOWP.R_EI_SYSADM.V_PPV_WTX_PO_RECEIPT_BY_VOUCHER;

*/
with
    cte_receipt as (
        select
            fact.po_order_number,
            fact.voucher,
            fact.source_item_identifier as source_item_identifier,
            fact.po_order_company as po_order_company,
            fact.po_received_date as po_received_date,
            to_date(calendar_month_start_dt) as calendar_date,
            fiscal_year,
            fact.transaction_currency as transaction_currency,
            receipt_quantity as receipt_received_quantity,
            invoiced_qty as invoiced_qty,
            invoiced_amount as invoiced_amount
        from {{ ref("stg_f_wbx_ppv_receipt_invoice_quantity") }} fact
        inner join
            {{ ref("src_dim_date") }} dim
            on to_date(fact.po_received_date) = dim.calendar_date
        where trim(fact.source_item_identifier) <> '' and fiscal_year >= '2019'
    ),
    cte_receipt_eur as (
        select
            fact.po_order_number,
            fact.voucher,
            fact.source_item_identifier as source_item_identifier,
            fact.po_order_company as po_order_company,
            fact.po_received_date as po_received_date,
            to_date(calendar_month_start_dt) as calendar_date,
            fiscal_year,
            fact.transaction_currency as transaction_currency,
            receipt_quantity as receipt_received_quantity,
            invoiced_qty as invoiced_qty
        from {{ ref("stg_f_wbx_ppv_receipt_invoice_quantity") }} fact
        inner join
            {{ ref("src_dim_date") }} dim
            on to_date(fact.po_received_date) = dim.calendar_date
        where
            trim(fact.source_item_identifier) <> ''
            and fiscal_year >= '2019'
            and fact.transaction_currency = 'EUR'
    ),
    cte_gl_ini as (
        select distinct
            f.source_system,
            reference_id as po_order_number,
            voucher,
            f.document_company,
            source_item_identifier,
            date_trunc('month', f.gl_date) gl_date,
            transaction_amount as gl_amount
        from {{ ref("src_mfg_wtx_wo_gl_fact") }} f  /* r_ei_sysadm.mfg_wtx_wo_gl_fact */
        inner join
            {{ ref("fct_wbx_fin_prc_po_receipt") }} r  /* R_EI_SYSADM.PRC_PO_RECEIPT_FACT R what is source for this model */
            on f.reference_id = r.po_order_number
        where
            f.source_account_identifier in ('540010', '540015', '540005')
            and source_item_identifier <> ' '
            and r.source_system = 'WEETABIX'
            and not (reference_id is null or reference_id = '')
    ),
    cte_gl as (
        select
            document_company,
            source_item_identifier,
            gl_date,
            po_order_number,
            voucher,
            sum(gl_amount) as gl_amount
        from cte_gl_ini
        group by 1, 2, 3, 4, 5
    ),
    cte_gl_invoice_ini as (
        select distinct
            f.source_system,
            reference_id as po_order_number,
            voucher,
            f.document_company,
            source_item_identifier,
            date_trunc('month', f.gl_date) gl_date,
            transaction_amount as gl_amount_invoice,
            row_number() over (
                partition by
                    f.document_company,
                    date_trunc('month', f.gl_date),
                    reference_id,
                    voucher
                order by reference_id
            ) rownum
        from {{ ref("src_mfg_wtx_wo_gl_fact") }} f  /* r_ei_sysadm.mfg_wtx_wo_gl_fact */
        inner join
            {{ ref("fct_wbx_fin_prc_po_receipt") }} r  -- r_ei_sysadm.prc_po_receipt_fact r 
            on f.reference_id = r.po_order_number
        where
            f.source_account_identifier in ('540010', '540015', '540005')
            and source_item_identifier <> ' '
            and r.source_system = 'WEETABIX'
            and substr(voucher, 1, 3) = 'MIN'
            and not (reference_id is null or reference_id = '')
    ),
    cte_gl_invoice as (
        select
            document_company,
            gl_date,
            po_order_number,
            voucher,
            case
                when max(rownum) = 0
                then 0
                else round((avg(gl_amount_invoice) / max(rownum)), 4)
            end as gl_amount_invoice
        from cte_gl_invoice_ini
        group by 1, 2, 3, 4
    ),
    cte_cost_bl_ini as (
        select
            itemid,
            upper(dataareaid) company_code,
            modifieddatetime,
            priceunit,
            price,
            fiscal_year_begin_dt,
            fiscal_year_end_dt,
            versionid,
            row_number() over (
                partition by
                    itemid, upper(dataareaid), fiscal_year_begin_dt, fiscal_year_end_dt
                order by (modifieddatetime) desc
            ) as row_no
        from {{ ref("src_inventitemprice") }}
        inner join
            {{ ref("src_dim_date") }} date
            on date.report_fiscal_year = right(versionid, 4)
        where versionid like 'StdBL%'
    ),
    cte_cost_bl as (
        select distinct
            itemid,
            company_code,
            round(priceunit, 4) as priceunit,
            year(modifieddatetime) year,
            to_date(fiscal_year_begin_dt) fiscal_year_begin_dt,
            to_date(fiscal_year_end_dt) fiscal_year_end_dt,
            round(price, 4) as standard_cost
        from cte_cost_bl_ini
        where row_no = 1
    ),
    cte_cost_co_ini as (
        select
            itemid,
            upper(dataareaid) company_code,
            modifieddatetime,
            priceunit,
            price,
            fiscal_year_begin_dt,
            fiscal_year_end_dt,
            versionid,
            row_number() over (
                partition by
                    itemid, upper(dataareaid), fiscal_year_begin_dt, fiscal_year_end_dt
                order by (modifieddatetime) desc
            ) as row_no
        from {{ ref("src_inventitemprice") }}
        inner join
            {{ ref("src_dim_date") }} date
            on date.report_fiscal_year = right(versionid, 4)
        where versionid like 'StdCo%'
    ),
    cte_cost_co as (
        select distinct
            itemid,
            company_code,
            round(priceunit, 4) as priceunit,
            year(modifieddatetime) year,
            to_date(fiscal_year_begin_dt) fiscal_year_begin_dt,
            to_date(fiscal_year_end_dt) fiscal_year_end_dt,
            round(price, 4) as standard_cost
        from cte_cost_co_ini
        where row_no = 1
    ),
    cte_cost_ini as (
        select
            itemid,
            upper(dataareaid) company_code,
            modifieddatetime,
            priceunit,
            price,
            versionid,
            row_number() over (
                partition by itemid, upper(dataareaid) order by (modifieddatetime) desc
            ) as row_no
        from {{ ref("src_inventitemprice") }}  -- weetabix.inventitemprice
        where versionid like 'Std%'
    ),
    cte_cost as (
        select distinct
            itemid,
            company_code,
            modifieddatetime,
            round(priceunit, 4) as priceunit,
            round(price, 4) as standard_cost
        from cte_cost_ini
        where row_no = 1
    ),
    cte_final as (
        select distinct
            receipt.po_order_number,
            receipt.voucher,
            receipt.source_item_identifier,
            receipt.po_order_company,
            receipt.po_received_date,
            receipt.calendar_date,
            receipt.fiscal_year,
            'GBP' as base_currency,
            receipt.transaction_currency as transaction_currency,
            coalesce(cost_bl.priceunit, cost_co.priceunit, cost.priceunit) priceunit,
            round(receipt.receipt_received_quantity, 4) as receipt_received_quantity,
            round(receipt.invoiced_qty, 4) as invoiced_qty,
            round(receipt.invoiced_amount, 4) as invoiced_amount,
            coalesce(
                cost_bl.standard_cost, cost_co.standard_cost, cost.standard_cost
            ) as standard_cost,
            round(
                case
                    when receipt.receipt_received_quantity = 0
                    then 0
                    else
                        (
                            (
                                coalesce(
                                    cost_bl.standard_cost,
                                    cost_co.standard_cost,
                                    cost.standard_cost
                                ) / round(
                                    coalesce(
                                        cost_bl.priceunit,
                                        cost_co.priceunit,
                                        cost.priceunit
                                    ),
                                    4
                                )
                                * receipt.receipt_received_quantity
                            )
                            + nvl(gl_amount, 0)
                        )
                        / round(receipt.receipt_received_quantity, 4)
                        * round(
                            coalesce(
                                cost_bl.priceunit, cost_co.priceunit, cost.priceunit
                            ),
                            4
                        )
                end,
                4
            ) as base_receipt_cost,
            round(
                nvl(
                    case
                        when receipt_eur.receipt_received_quantity = 0
                        then 0
                        else
                            (
                                (
                                    coalesce(
                                        cost_bl.standard_cost,
                                        cost_co.standard_cost,
                                        cost.standard_cost
                                    ) / coalesce(
                                        cost_bl.priceunit,
                                        cost_co.priceunit,
                                        cost.priceunit
                                    )
                                    * receipt_eur.receipt_received_quantity
                                )
                                + nvl(gl_amount, 0)
                            )
                            / receipt_eur.receipt_received_quantity
                            * round(
                                coalesce(
                                    cost_bl.priceunit, cost_co.priceunit, cost.priceunit
                                ),
                                4
                            )
                    end,
                    0
                ),
                4
            )
            * nvl(curr_conv_rt, 1) as eur_receipt_cost,
            round(
                nvl(receipt_eur.receipt_received_quantity, 0), 4
            ) as eur_receipt_received_quantity,
            round(
                (
                    coalesce(
                        cost_bl.standard_cost, cost_co.standard_cost, cost.standard_cost
                    )
                    / coalesce(cost_bl.priceunit, cost_co.priceunit, cost.priceunit)
                )
                * receipt.receipt_received_quantity,
                4
            ) as std_receipt_received_amount,
            case
                when substr(receipt.voucher, 1, 3) = 'MIN'
                then 0
                else round(nvl(gl_amount, 0), 4)
            end as gl_adjustment_receipt,
            round(nvl(gl_invoice.gl_amount_invoice, 0), 4) gl_adjustment_invoice,
            nvl(curr_conv_rt, 1) as curr_conv_rt
        from cte_receipt receipt
        left outer join
            -- ---------Receipt Cost for
            -- EUR---------------------------------------------------------
            cte_receipt_eur receipt_eur  -- second cte
            on receipt_eur.po_order_number = receipt.po_order_number
            and receipt_eur.po_order_company = receipt.po_order_company
            and receipt_eur.calendar_date = receipt.calendar_date
            and receipt_eur.transaction_currency = receipt.transaction_currency
            and receipt_eur.voucher = receipt.voucher
            and receipt_eur.source_item_identifier = receipt.source_item_identifier
        -- -----------------------------------------GL Adjustment
        -- Receipt---------------------
        left outer join
            cte_gl gl  -- third cte
            on gl.source_item_identifier = receipt.source_item_identifier
            and gl.document_company = receipt.po_order_company
            and gl.po_order_number = receipt.po_order_number
            and gl.voucher = receipt.voucher

        -- -----------------------------------------GL Adjustment
        -- Invoice---------------------
        left outer join
            cte_gl_invoice gl_invoice  -- 4th cte
            on gl_invoice.document_company = receipt.po_order_company
            and gl_invoice.po_order_number = receipt.po_order_number
            and gl_invoice.voucher = receipt.voucher

        -- ---------Pick cost for BL----------------
        left outer join
            cte_cost_bl cost_bl  -- 5th cte
            on cost_bl.itemid = receipt.source_item_identifier
            and cost_bl.company_code = receipt.po_order_company
            and to_date(receipt.po_received_date) >= cost_bl.fiscal_year_begin_dt
            and to_date(receipt.po_received_date) <= cost_bl.fiscal_year_end_dt
        -- ---------Pick cost for CO----------------
        left outer join
            cte_cost_co cost_co  -- 6th cte
            on cost_co.itemid = receipt.source_item_identifier
            and cost_co.company_code = receipt.po_order_company
            and to_date(receipt.po_received_date) >= cost_co.fiscal_year_begin_dt
            and to_date(receipt.po_received_date) <= cost_co.fiscal_year_end_dt
        -- ------------------Pick cost for All the Item without joining on Fiscal
        -- period------------
        left outer join
            cte_cost cost  -- 7th cte
            on cost.itemid = receipt.source_item_identifier
            and cost.company_code = receipt.po_order_company
        -- ---------------Currency Conversion
        -- Rate------------------------------------------
        left join
            {{ ref("src_currency_exch_rate_dly_dim") }} curr  -- EI_RDM.CURRENCY_EXCH_RATE_DLY_DIM CURR
            on curr.curr_to_code = receipt.transaction_currency
            and dateadd(day, 1, to_date(curr.eff_from_d)) = receipt.po_received_date
            and curr.curr_from_code = 'GBP'
    )
select *
from cte_final
