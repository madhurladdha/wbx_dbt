{{
  config(
      tags=["procurement","ppv"]
  )
}}


/*
compared view to view and just 460 records of difference 
select count(*) from int_f_wbx_prc_ppv; --103,524
select count(*) from  postsnowp.R_EI_SYSADM.V_PRC_WTX_PPV_FACT_UBER;

select * from int_f_wbx_prc_ppv --469
minus
select * from  postsnowp.R_EI_SYSADM.V_PRC_WTX_PPV_FACT_UBER;

select * from postsnowp.R_EI_SYSADM.V_PRC_WTX_PPV_FACT_UBER --469
minus
select * from  int_f_wbx_prc_ppv;

*/

with
    cte_item as (
        select distinct
            itm.source_item_identifier,
            plant.company_code,
            itm.item_type,
            itm.primary_uom,
            itm.buyer_code,
            to_date(fiscal_period_begin_dt) as calendar_date
        from {{ ref("dim_wbx_item") }} itm
        inner join
            {{ ref("dim_wbx_plant_dc") }} plant
            on itm.source_business_unit_code = plant.source_business_unit_code
            and itm.source_system = plant.source_system
        inner join {{ ref("src_dim_date") }} on fiscal_year = year(current_date)
        where
            itm.source_system = 'WEETABIX'
            and itm.buyer_code is not null
            and itm.item_class
            in ('WHEAT', 'RAWMATS', 'PACKAGING', 'STRETCH', 'NONBOM', '3RDPARTY')
    ),
    cte_receipt as (
        select
            source_item_identifier,
            po_order_company,
            calendar_date,
            max(po_received_date) as po_received_date,
            max(standard_cost) as standard_cost,
            case
                when sum(receipt_received_quantity) = 0
                then 0
                else
                    round(
                        sum(receipt_received_quantity * base_receipt_cost)
                        / sum(receipt_received_quantity),
                        4
                    )
            end as base_receipt_cost,
            sum(receipt_received_quantity) as receipt_received_quantity,
            sum(gl_adjustment_receipt) as gl_adjustment_receipt,
            sum(gl_adjustment_invoice) as gl_adjustment_invoice,
            base_currency as base_currency,
            max(transaction_currency) as txn_currency,
            max(priceunit) as price_unit,
            max(eur_receipt_cost) as eur_receipt_cost,
            sum(eur_receipt_received_quantity) as eur_receipt_received_quantity,
            max(curr_conv_rt) as curr_conv_rt
        from {{ ref("stg_f_wbx_ppv_po_receipt_by_voucher") }}
        group by source_item_identifier, po_order_company, calendar_date, base_currency
    ),
    cte_ly_receipt as (
        select
            source_item_identifier,
            po_order_company,
            calendar_date,
            max(po_received_date) as po_received_date,
            max(standard_cost) as standard_cost,
            case
                when sum(receipt_received_quantity) = 0
                then 0
                else
                    round(
                        sum(receipt_received_quantity * base_receipt_cost)
                        / sum(receipt_received_quantity),
                        4
                    )
            end as base_receipt_cost,
            sum(receipt_received_quantity) as receipt_received_quantity,
            sum(gl_adjustment_receipt) as gl_adjustment_receipt,
            sum(gl_adjustment_invoice) as gl_adjustment_invoice,
            base_currency as base_currency,
            max(transaction_currency) as txn_currency,
            max(priceunit) as price_unit,
            max(eur_receipt_cost) as eur_receipt_cost,
            sum(eur_receipt_received_quantity) as eur_receipt_received_quantity,
            max(curr_conv_rt) as curr_conv_rt
        from {{ ref("stg_f_wbx_ppv_po_receipt_by_voucher") }}
        group by source_item_identifier, po_order_company, calendar_date, base_currency
    ),
    cte_budget as (
        select
            source_item_identifier,
            company_code,
            scenario,
            version_dt,
            forcast_year,
            calendar_date,
            buyer_code,
            quantity,
            price
        from {{ ref("fct_wbx_prc_ppv_budget") }}  -- R_EI_SYSADM.PRC_WTX_BUDGET_FACT 
        where
            version_dt
            = (select max(version_dt) from {{ ref("fct_wbx_prc_ppv_budget") }})
    ),
    cte_budget_inflation as (
        select buyer_code, inflation_year, version_dt, calendar_date, inflation
        from {{ ref("fct_wbx_prc_ppv_item_inflation") }}
        where
            scenario = 'BUDGET'
            and version_dt = (
                select max(version_dt)
                from {{ ref("fct_wbx_prc_ppv_item_inflation") }}
                where scenario = 'BUDGET'
            )
    ),
    cte_budget_exch_rate as (
        select exch_rate_year, version_dt, scenario, calendar_date, exchange_rate
        from {{ ref("dim_wbx_prc_ppv_forecast_exch_rate") }}
        where
            (version_dt) in (
                select max(version_dt)
                from {{ ref("dim_wbx_prc_ppv_forecast_exch_rate") }}
                where scenario = 'BUDGET'
            )
    ),
    cte_forecast as (
        select
            source_item_identifier,
            company_code,
            scenario,
            version_dt,
            forecast_year,
            calendar_date,
            buyer_code,
            quantity,
            price
        from {{ ref("fct_wbx_prc_ppv_forecast") }}
        where
            version_dt
            = (select max(version_dt) from {{ ref("fct_wbx_prc_ppv_forecast") }})
    ),
    cte_forecast_exch_rate as (
        select exch_rate_year, version_dt, scenario, calendar_date, exchange_rate
        from {{ ref("dim_wbx_prc_ppv_forecast_exch_rate") }}
        where
            (version_dt) in (
                select max(version_dt)
                from {{ ref("dim_wbx_prc_ppv_forecast_exch_rate") }}
                where scenario = 'LIVE'
            )
    ),
    cte_forecast_inflation as (
        select buyer_code, inflation_year, version_dt, calendar_date, inflation
        from {{ ref("fct_wbx_prc_ppv_item_inflation") }}
        where
            scenario = 'LIVE'
            and version_dt = (
                select max(version_dt)
                from {{ ref("fct_wbx_prc_ppv_item_inflation") }}
                where scenario = 'LIVE'
            )
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
        from {{ ref("src_inventitemprice") }}
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
    cte_item_level as (
        select *
        from
            (
                select
                    'ITEM_LEVEL' as source_content_filter,
                    null as po_order_number,
                    null as voucher,
                    trunc(current_date, 'MM') as version_dt,
                    item.source_item_identifier,
                    coalesce(
                        receipt.price_unit,
                        cost_bl.priceunit,
                        cost_co.priceunit,
                        cost.priceunit
                    ) as price_unit,
                    item.company_code,
                    item.item_type,
                    item.primary_uom,
                    item.buyer_code,
                    item.calendar_date,
                    receipt.base_currency,
                    receipt.txn_currency,
                    to_date(receipt.po_received_date) as po_received_date,
                    coalesce(
                        receipt.standard_cost,
                        cost_bl.standard_cost,
                        cost_co.standard_cost,
                        cost.standard_cost
                    ) as cy_standard_cost,
                    receipt.base_receipt_cost as cy_base_receipt_cost,
                    receipt.receipt_received_quantity as cy_receipt_received_quantity,
                    receipt.gl_adjustment_receipt as cy_gl_adjustment_receipt,
                    receipt.eur_receipt_cost as cy_eur_receipt_cost,
                    receipt.eur_receipt_received_quantity
                    as cy_eur_receipt_received_quantity,
                    receipt.curr_conv_rt as cy_curr_conv_rt,
                    ly_receipt.standard_cost as ly_standard_cost,
                    ly_receipt.base_receipt_cost as ly_base_receipt_cost,
                    ly_receipt.receipt_received_quantity
                    as ly_receipt_received_quantity,
                    ly_receipt.gl_adjustment_receipt as ly_gl_adjustment_receipt,
                    ly_receipt.eur_receipt_cost as ly_eur_receipt_cost,
                    ly_receipt.eur_receipt_received_quantity
                    as ly_eur_receipt_received_quantity,
                    ly_receipt.curr_conv_rt as ly_curr_conv_rt,
                    budget.version_dt budget_version_date,
                    budget.forcast_year budget_forcast_year,
                    budget.quantity budget_quantity,
                    budget.price budget_price,
                    budget_exch_rate.exchange_rate as budget_exchange_rate,
                    forecast.version_dt forecast_version_date,
                    forecast.forecast_year forecast_year,
                    forecast.quantity forecast_quantity,
                    forecast.price forecast_price,
                    forecast_exch_rate.exchange_rate as forecast_exchange_rate,
                    budget_inflation.inflation as budget_inflation,
                    forecast_inflation.inflation as forecast_inflation,
                    to_date(current_date) as load_date,
                    null as invoiced_qty,
                    null as invoiced_amount,
                    receipt.gl_adjustment_invoice as cy_gl_adjustment_invoice,
                    ly_receipt.gl_adjustment_invoice as ly_gl_adjustment_invoice
                -- --------Fetch all the Item for Weetabix-------------------
                from
                    (
                        cte_item item
                        -- -------------Actuals for
                        -- CY------------------------------------
                        left join
                            cte_receipt receipt  -- second cte
                            on receipt.source_item_identifier
                            = item.source_item_identifier
                            and receipt.po_order_company = item.company_code
                            and receipt.calendar_date = item.calendar_date
                        -- -------------Actuals for
                        -- LY------------------------------------
                        left join
                            cte_ly_receipt ly_receipt  -- --third cte
                            on ly_receipt.source_item_identifier
                            = item.source_item_identifier
                            and ly_receipt.po_order_company = item.company_code
                            and ly_receipt.calendar_date
                            = dateadd(year, -1, item.calendar_date)
                        -- -------------------------Budget-----------------------------------------
                        left outer join
                            cte_budget budget  -- 4th cte
                            on budget.source_item_identifier
                            = item.source_item_identifier
                            and budget.company_code = item.company_code
                            and budget.calendar_date = item.calendar_date
                        -- ---------------------Budget Inflation Rate--------------
                        left outer join
                            cte_budget_inflation budget_inflation  -- 5th cte
                            on budget_inflation.inflation_year = budget.forcast_year
                            and budget_inflation.calendar_date = budget.calendar_date
                            and budget_inflation.buyer_code = budget.buyer_code
                        -- ---------------------BUDGET Exchange Rate--------------
                        left outer join
                            cte_budget_exch_rate budget_exch_rate  -- 6th  cte
                            on budget_exch_rate.scenario = budget.scenario
                            and budget_exch_rate.exch_rate_year = budget.forcast_year
                            and budget_exch_rate.calendar_date = budget.calendar_date
                        -- -------------------------FORECAST-----------------------------------------
                        left outer join
                            cte_forecast forecast  -- 7th cte
                            on forecast.source_item_identifier
                            = item.source_item_identifier
                            and forecast.company_code = item.company_code
                            and forecast.calendar_date = item.calendar_date
                        -- ---------------------FORECAST Exchange Rate--------------
                        left outer join
                            cte_forecast_exch_rate forecast_exch_rate  -- 8th cte
                            on forecast_exch_rate.scenario = forecast.scenario
                            and forecast_exch_rate.exch_rate_year
                            = forecast.forecast_year
                            and forecast_exch_rate.calendar_date
                            = forecast.calendar_date
                        -- ---------------------FORECAST Inflation Rate--------------
                        left outer join
                            cte_forecast_inflation forecast_inflation  -- 9th cte
                            on forecast_inflation.inflation_year
                            = forecast.forecast_year
                            and forecast_inflation.calendar_date
                            = forecast.calendar_date
                            and forecast_inflation.buyer_code = forecast.buyer_code
                    )  -- 9th cte, these all should be closed inside one from 
                -- ---------------------Derive Standart cost which dont have
                -- actuals--------------
                -- ---------Pick cost for BL----------------
                left outer join
                    cte_cost_bl cost_bl  -- 10th cte
                    on cost_bl.itemid = budget.source_item_identifier
                    and cost_bl.company_code = budget.company_code
                    and to_date(budget.version_dt) >= cost_bl.fiscal_year_begin_dt
                    and to_date(budget.version_dt) <= cost_bl.fiscal_year_end_dt
                -- ---------Pick cost for CO----------------
                left outer join
                    cte_cost_co cost_co  -- 11th cte
                    on cost_co.itemid = budget.source_item_identifier
                    and cost_co.company_code = budget.company_code
                    and to_date(budget.version_dt) >= cost_co.fiscal_year_begin_dt
                    and to_date(budget.version_dt) <= cost_co.fiscal_year_end_dt
                -- ------------------Pick cost for All the Item without joining on
                -- Fiscal period------------
                left outer join
                    cte_cost cost  -- 12th cte
                    on cost.itemid = budget.source_item_identifier
                    and cost.company_code = budget.company_code
            )
        -- -------------------Filter the records where all values are
        -- null------------------
        where
            (
                base_currency is not null
                or po_received_date is not null
                or cy_standard_cost is not null
                or cy_base_receipt_cost is not null
                or cy_receipt_received_quantity is not null
                or cy_gl_adjustment_receipt is not null
                or budget_version_date is not null
                or budget_forcast_year is not null
                or budget_quantity is not null
                or budget_price is not null
                or budget_exchange_rate is not null
                or forecast_version_date is not null
                or forecast_year is not null
                or forecast_quantity is not null
                or forecast_price is not null
                or forecast_exchange_rate is not null
                or budget_inflation is not null
                or forecast_inflation is not null
            )
    ),
    cte_item_fa as (
        select distinct
            itm.source_item_identifier,
            plant.company_code,
            itm.item_type,
            itm.primary_uom,
            itm.buyer_code
        from {{ ref("dim_wbx_item") }} itm
        inner join
            {{ ref("dim_wbx_plant_dc") }} plant
            on itm.source_business_unit_code = plant.source_business_unit_code
            and itm.source_system = plant.source_system
        where
            itm.source_system = 'WEETABIX'
            and itm.buyer_code is not null
            and itm.item_class
            in ('WHEAT', 'RAWMATS', 'PACKAGING', 'STRETCH', 'NONBOM', '3RDPARTY')
    ),
    cte_voucher_level as (
        select
            'VOUCHER_LEVEL' as source_content_filter,
            voucher.po_order_number,
            voucher,
            trunc(current_date, 'MM') as version_dt,
            voucher.source_item_identifier,
            priceunit as price_unit,
            voucher.po_order_company as company_code,
            item.item_type as item_type,
            item.primary_uom as primary_uom,
            item.buyer_code as buyer_code,
            voucher.calendar_date,
            base_currency,
            transaction_currency as txn_currency,
            po_received_date,
            standard_cost as cy_standard_cost,
            base_receipt_cost as cy_base_receipt_cost,
            receipt_received_quantity as cy_receipt_received_quantity,
            gl_adjustment_receipt as cy_gl_adjustment_receipt,
            eur_receipt_cost as cy_eur_receipt_cost,
            eur_receipt_received_quantity as cy_eur_receipt_received_quantity,
            curr_conv_rt as cy_curr_conv_rt,
            null as ly_standard_cost,
            null as ly_base_receipt_cost,
            null as ly_receipt_received_quantity,
            null as ly_gl_adjustment_receipt,
            null as ly_eur_receipt_cost,
            null as ly_eur_receipt_received_quantity,
            null as ly_curr_conv_rt,
            null as budget_version_date,
            null as budget_forcast_year,
            null as budget_quantity,
            null as budget_price,
            null as budget_exchange_rate,
            null as forecast_version_date,
            null as forecast_year,
            null as forecast_quantity,
            null as forecast_price,
            null as forecast_exchange_rate,
            null as budget_inflation,
            null as forecast_inflation,
            null as load_date,
            invoiced_qty,
            invoiced_amount,
            gl_adjustment_invoice,
            null
        from {{ ref("stg_f_wbx_ppv_po_receipt_by_voucher") }} voucher
        inner join
            cte_item_fa item
            on voucher.source_item_identifier = item.source_item_identifier
            and voucher.po_order_company = item.company_code
        where fiscal_year >= '2019'
    ),
    cte_final as (
        select *
        from cte_item_level
        union all
        select *
        from cte_voucher_level
    )
select *
from cte_final
