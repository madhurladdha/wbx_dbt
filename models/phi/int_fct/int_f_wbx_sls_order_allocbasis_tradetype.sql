{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        on_schema_change="sync_all_columns",
        tags=["sales", "gl", "dni","sales_gl_allocations"],
        pre_hook="""
                                        {{ truncate_if_exists(this.schema, this.table) }}
                                        """
    )
}}

/*  Adjustments once all dbt source models are ready:
    1) Replace the main source model once the Sales Order Fact is ready. Changes from conv_sls_wbx_slsorder_fact to fct_wbx_sls_order
    2) Update the joins on "old" guids back to the regular guids.
*/

/*
2024-01-30
change to truncate and load from CTAS
--Added company code in model and in all the calculations.
*/
with
source as (select * from {{ ref("fct_wbx_sls_order") }}),
item_ext as (select * from {{ ref("dim_wbx_item_ext") }}),
customer_ext as (select * from {{ ref("dim_wbx_customer_ext") }}),

aggregate as (
        select
        f.source_item_identifier,
        f.item_guid,
        f.sales_order_company as company_code,
        ix.product_class_code,
        cx.trade_type_code,
        date_trunc('month', f.line_gl_date) gl_month,
        cx.market_code,
        cx.sub_market_code,
        ix.branding_code,
        sum(f.shipped_kg_quantity) as tot_shipped_kg_quantity,
        sum(f.shipped_ca_quantity) as tot_ca_kg_quantity
    from source f
    left join
        customer_ext cx
        on f.ship_customer_address_guid = cx.customer_address_number_guid
    left join
        item_ext ix
        on f.item_guid = ix.item_guid
        and f.source_item_identifier = ix.source_item_identifier
        and f.source_business_unit_code = ix.source_business_unit_code
    group by
        f.source_item_identifier,
        f.item_guid,
        f.sales_order_company,
        ix.product_class_code,
        cx.trade_type_code,
        date_trunc('month', f.line_gl_date),
        cx.market_code,
        cx.sub_market_code,
        ix.branding_code
    having tot_shipped_kg_quantity > 0
)

select
    '{{env_var("DBT_SOURCE_SYSTEM")}}' as source_system,
    trade_type_code,
    source_item_identifier,
    item_guid,
    company_code,
    product_class_code,
    market_code,
    sub_market_code,
    branding_code,
    gl_month,
    tot_ca_kg_quantity,
    tot_shipped_kg_quantity,
    sum(tot_shipped_kg_quantity) over (partition by item_guid, gl_month,company_code) as tot_kg_item,
    round(
        tot_shipped_kg_quantity
        / sum(tot_shipped_kg_quantity) over (partition by item_guid, gl_month,company_code),
        8
    ) as perc_item,
    sum(tot_shipped_kg_quantity) over (
        partition by trade_type_code, item_guid, gl_month,company_code
    ) as tot_kg_trade_type_item,
    round(
        tot_shipped_kg_quantity / sum(tot_shipped_kg_quantity) over (
            partition by trade_type_code, item_guid, gl_month,company_code
        ),
        8
    ) as perc_trade_type_item,

    -- -no customer or item information---
    sum(tot_shipped_kg_quantity) over (partition by gl_month,company_code) as tot_kg_month,
    round(
        tot_shipped_kg_quantity
        / sum(tot_shipped_kg_quantity) over (partition by gl_month,company_code),
        8
    ) as perc_month,
    -- -trade type but no product class or item information---
    sum(tot_shipped_kg_quantity) over (
        partition by trade_type_code, gl_month,company_code
    ) as tot_kg_trade_type,
    round(
        tot_shipped_kg_quantity
        / sum(tot_shipped_kg_quantity) over (partition by trade_type_code, gl_month,company_code),
        8
    ) as perc_trade_type,
    -- -product class but no item or customer information---
    sum(tot_shipped_kg_quantity) over (
        partition by product_class_code, gl_month,company_code
    ) as tot_kg_product_class,
    round(
        tot_shipped_kg_quantity
        / sum(tot_shipped_kg_quantity) over (partition by product_class_code, gl_month,company_code),
        8
    ) as perc_product_class,
    -- -trade type and product class but no item or customer ship-to----
    sum(tot_shipped_kg_quantity) over (
        partition by trade_type_code, product_class_code, gl_month,company_code
    ) as tot_kg_trade_type_product_class,
    round(
        tot_shipped_kg_quantity / sum(tot_shipped_kg_quantity) over (
            partition by trade_type_code, product_class_code, gl_month,company_code
        ),
        8
    ) as perc_trade_type_product_class,
    -- -product branding code from cost center but no customer or item information---
    sum(tot_shipped_kg_quantity) over (
        partition by branding_code, gl_month,company_code
    ) as tot_kg_branding_code,
    round(
        tot_shipped_kg_quantity
        / sum(tot_shipped_kg_quantity) over (partition by branding_code, gl_month,company_code),
        8
    ) as perc_branding_code,
    -- -market code from cost center but no trade type or customer or product level
    -- info---
    sum(tot_shipped_kg_quantity) over (
        partition by market_code, gl_month,company_code
    ) as tot_kg_market_code,
    round(
        tot_shipped_kg_quantity
        / sum(tot_shipped_kg_quantity) over (partition by market_code, gl_month,company_code),
        8
    ) as perc_market_code,
    -- -Sub-market code from cost center but no trade type or customer or product
    -- level info---
    sum(tot_shipped_kg_quantity) over (
        partition by sub_market_code, gl_month,company_code
    ) as tot_kg_submarket,
    round(
        tot_shipped_kg_quantity
        / sum(tot_shipped_kg_quantity) over (partition by sub_market_code, gl_month,company_code),
        8
    ) as perc_submarket,
    -- -market code from cost center but no trade type or custoemr and product class---
    sum(tot_shipped_kg_quantity) over (
        partition by market_code, product_class_code, gl_month,company_code
    ) as tot_kg_market_code_product_class,
    round(
        tot_shipped_kg_quantity / sum(tot_shipped_kg_quantity) over (
            partition by market_code, product_class_code, gl_month,company_code
        ),
        8
    ) as perc_market_code_product_class,
    -- -sub-market code from cost center but no trade type or custoemr and product
    -- class---
    sum(tot_shipped_kg_quantity) over (
        partition by sub_market_code, product_class_code, gl_month,company_code
    ) as tot_kg_submarket_product_class,
    round(
        tot_shipped_kg_quantity / sum(tot_shipped_kg_quantity) over (
            partition by sub_market_code, product_class_code, gl_month,company_code
        ),
        8
    ) as perc_submarket_product_class,
    -- - trade type and branding ---
    sum(tot_shipped_kg_quantity) over (
        partition by trade_type_code, branding_code, gl_month,company_code
    ) as tot_kg_trade_type_branding_code,
    round(
        tot_shipped_kg_quantity / sum(tot_shipped_kg_quantity) over (
            partition by trade_type_code, branding_code, gl_month,company_code
        ),
        8
    ) as perc_trade_type_branding_code,
    -- -market code and branding---
    sum(tot_shipped_kg_quantity) over (
        partition by market_code, branding_code, gl_month,company_code
    ) as tot_kg_market_code_branding_code,
    round(
        tot_shipped_kg_quantity / sum(tot_shipped_kg_quantity) over (
            partition by market_code, branding_code, gl_month,company_code
        ),
        8
    ) as perc_market_code_branding_code,
    -- -submarket and branding---
    sum(tot_shipped_kg_quantity) over (
        partition by sub_market_code, branding_code, gl_month,company_code
    ) as tot_kg_submarket_branding_code,
    round(
        tot_shipped_kg_quantity / sum(tot_shipped_kg_quantity) over (
            partition by sub_market_code, branding_code, gl_month,company_code
        ),
        8
    ) as perc_submarket_branding_code,
    trunc(current_date, 'DD') as load_date,
    trunc(current_date, 'DD') as update_date
from aggregate