{{
    config(
        materialized=env_var("DBT_MAT_TABLE"),
        enabled=false,
        tags=["sales", "pcos", "bom"],
        unique_key="UNIQUE_KEY",
        on_schema_change="sync_all_columns",
        pre_hook="""
        {% set now = modules.datetime.datetime.now() %}
        {%- set full_load_day -%} {{env_var('DBT_FULL_LOAD_DAY')}} {%- endset -%}
        {%- set day_today -%} {{ now.strftime('%A') }} {%- endset -%}
        {% if day_today == full_load_day %}
        {{ truncate_if_exists(this.schema, this.table) }}
        {% endif %}
        """,
    )
}}

/*  This model is not actively used downstream in the IICS world.  So, we have converted the code, but are disabling the model as it is not currently required.
*/

with
    cbom_fct as (select * from {{ ref("fct_wbx_mfg_cbom") }}),
    item as (select * from {{ ref("dim_wbx_item") }}),
    item_ext as (select * from {{ ref("dim_wbx_item_ext") }}),
    /*The Sales Order Fact needs to be swapped out with the proper new model once that is complete. */
    slsorder as (select * from {{ ref("src_sls_wbx_slsorder_fact") }}),
    cbom as (
        select
            source_system,
            root_company_code,
            stock_site,
            version_id,
            eff_date,
            creation_date_time,
            expir_date,
            root_src_item_identifier,
            root_src_variant_code,
            root_src_unit_price,
            gl_unit_price,
            case
                stock_site
                when 'WBX-CBY'
                then round(nvl("'CG'", 0), numdecs)
                else nvl("'CG'", 0)
            end as raw_materials,
            case
                stock_site
                when 'WBX-CBY'
                then round(nvl("'CG_PACK'", 0), numdecs)
                else nvl("'CG_PACK'", 0)
            end as packaging,
            case
                stock_site
                when 'WBX-CBY'
                then round(nvl("'LAB'", 0), numdecs)
                else nvl("'LAB'", 0)
            end as labour,
            case
                stock_site
                when 'WBX-CBY'
                then round(nvl("'BI'", 0), numdecs)
                else nvl("'BI'", 0)
            end as bought_in,
            case
                stock_site
                when 'WBX-CBY'
                then round(nvl("'CO'", 0), numdecs)
                else nvl("'CO'", 0)
            end as co_pack,
            case
                stock_site
                when 'WBX-CBY'
                then
                    round(
                        gl_unit_price - (
                            round(
                                round(nvl("'CG'", 0), numdecs)
                                + round(nvl("'CG_PACK'", 0), numdecs)
                                + round(nvl("'LAB'", 0), numdecs)
                                + round(nvl("'BI'", 0), numdecs)
                                + round(nvl("'CO'", 0), numdecs),
                                numdecs
                            )
                        ),
                        numdecs
                    )
                else 0
            end as other,
            case
                min(root_src_variant_code) over (
                    partition by
                        source_system, root_company_code, root_src_item_identifier
                    order by
                        eff_date desc range between current row and unbounded following
                )
                when ''
                then 0
                else 1
            end as variant_flag
        from
            (
                select
                    source_system,
                    root_company_code,
                    stock_site,
                    version_id,
                    nvl(x.bl_eff_date, x.eff_date) as eff_date,
                    creation_date_time,
                    expir_date,
                    root_src_item_identifier,
                    root_src_variant_code,
                    x.numdecs,
                    upper(
                        case
                            when x.comp_cost_group_id = '' and x.comp_bom_level = 0
                            then 'BI'
                            when x.comp_cost_group_id = '' and x.comp_bom_level <> 0
                            then 'CO'
                            when upper(x.comp_cost_group_id) = 'MO'
                            then 'CG'
                            when upper(x.comp_cost_group_id) = 'TRAN'
                            then 'CG'
                            else x.comp_cost_group_id
                        end
                    ) as comp_cost_group_id,
                    max(root_src_unit_price) as root_src_unit_price,
                    max(round(root_src_unit_price, numdecs)) as gl_unit_price,
                    sum(
                        case
                            when x.comp_cost_group_id = '' and x.comp_bom_level = 0
                            then root_src_unit_price
                            else comp_item_unit_cost
                        end
                    ) as comp_item_unit_cost
                from
                    -- Derived CBY table to find Corby BOM's and to also find the BL
                    -- effective date for the Corby SKU
                    (
                        select
                            bom.*,
                            itm.mangrpcd_copack_flag,
                            cby.bl_eff_date,
                            nvl(cby.numdecs, 2) as numdecs
                        from cbom_fct bom
                        left outer join
                            (
                                select distinct
                                    right(f1.version_id, 4) as verison_year,
                                    f1.root_src_item_identifier,
                                    round(
                                        f1.root_src_unit_price, 2
                                    ) as root_src_unit_price,
                                    f1.eff_date,
                                    first_value(f2.eff_date) over (
                                        partition by
                                            right(f1.version_id, 4),
                                            f1.root_src_item_identifier,
                                            round(f1.root_src_unit_price, 2),
                                            f1.eff_date
                                        order by
                                            abs(
                                                datediff(
                                                    second, f1.eff_date, f2.eff_date
                                                )
                                            )
                                    ) as bl_eff_date,
                                    case
                                        when
                                            len(
                                                reverse(
                                                    cast(
                                                        floor(
                                                            reverse(
                                                                abs(
                                                                    f2.root_src_unit_price
                                                                )
                                                            )
                                                        ) as bigint
                                                    )
                                                )
                                            )
                                            < 2
                                        then 2
                                        else
                                            len(
                                                reverse(
                                                    cast(
                                                        floor(
                                                            reverse(
                                                                abs(
                                                                    f2.root_src_unit_price
                                                                )
                                                            )
                                                        ) as bigint
                                                    )
                                                )
                                            )
                                    end as numdecs
                                from cbom_fct f1
                                inner join
                                    cbom_fct f2
                                    on right(f1.version_id, 4) = right(f2.version_id, 4)
                                    and f1.root_src_item_identifier
                                    = f2.root_src_item_identifier
                                    and round(f1.root_src_unit_price, 2)
                                    = round(f2.root_src_unit_price, 2)
                                    and upper(f2.stock_site) = 'WBX-BL'
                                where
                                    upper(f1.stock_site) in ('WBX-CBY')
                                    and f1.comp_bom_level <> 0
                            ) as cby
                            on right(bom.version_id, 4) = cby.verison_year
                            and bom.root_src_item_identifier
                            = cby.root_src_item_identifier
                            and round(bom.root_src_unit_price, 2)
                            = cby.root_src_unit_price
                            and (
                                bom.eff_date = cby.eff_date
                                or bom.eff_date = cby.bl_eff_date
                            )
                        -- Derived table ITM, likely to change once updates to item
                        -- tables have been completed
                        inner join
                            (
                                select
                                    im.source_item_identifier,
                                    max(
                                        case
                                            when
                                                nvl(upper(e.mangrpcd_site), 'BL') in (
                                                    'BL',
                                                    'BL/CBY',
                                                    'CBY',
                                                    'WEETABIX/ORG'
                                                )
                                            then 'N'
                                            else 'Y'
                                        end
                                    ) as mangrpcd_copack_flag
                                from item im
                                left outer join item_ext e on im.item_guid = e.item_guid
                                where
                                    im.source_system = 'WEETABIX'
                                    and is_real(
                                        try_to_numeric(im.source_item_identifier)
                                    )
                                    = 1
                                    and len(im.source_item_identifier) = 5
                                group by im.source_item_identifier
                            ) itm
                            on bom.root_src_item_identifier = itm.source_item_identifier
                        where
                            (
                                (
                                    upper(bom.stock_site) not in ('WBX-CBY')
                                    and cby.root_src_item_identifier is null
                                )
                                or (
                                    upper(bom.stock_site) = 'WBX-CBY'
                                    and comp_bom_level <> 0
                                    and cby.root_src_item_identifier is not null
                                )
                            )
                            and (
                                (
                                    comp_bom_level = 1
                                    and (
                                        upper(comp_calctype_desc) in ('ITEM', 'SERVICE')
                                        or (
                                            upper(comp_calctype_desc) in ('BOM')
                                            and upper(parent_item_indicator)
                                            in ('ITEM', 'PARENT')
                                        )
                                    )
                                )
                                or (
                                    comp_bom_level = 0
                                    and upper(comp_calctype_desc) in ('PRODUCTION')
                                    and upper(parent_item_indicator) = 'ITEM'
                                )
                            )
                    ) x
                group by
                    source_system,
                    root_company_code,
                    stock_site,
                    version_id,
                    nvl(x.bl_eff_date, x.eff_date),
                    creation_date_time,
                    expir_date,
                    root_src_item_identifier,
                    root_src_variant_code,
                    x.numdecs,
                    upper(
                        case
                            when x.comp_cost_group_id = '' and x.comp_bom_level = 0
                            then 'BI'
                            when x.comp_cost_group_id = '' and x.comp_bom_level <> 0
                            then 'CO'
                            when upper(x.comp_cost_group_id) = 'MO'
                            then 'CG'
                            when upper(x.comp_cost_group_id) = 'TRAN'
                            then 'CG'
                            else x.comp_cost_group_id
                        end
                    )
            ) as sourcetable pivot (
                sum(comp_item_unit_cost)
                for comp_cost_group_id in ('CG', 'CG_PACK', 'LAB', 'BI', 'CO', 'OTH')
            ) as pivottable
    )

select distinct
    cast(
        {{
            dbt_utils.surrogate_key(
                [
                    "sl.SOURCE_SYSTEM",
                    "sl.SALES_LINE_NUMBER",
                    "sl.SALES_ORDER_NUMBER",
                    "sl.SOURCE_SALES_ORDER_TYPE",
                    "sl.SALES_ORDER_TYPE",
                    "sl.SALES_ORDER_COMPANY",
                    "sl.LINE_INVOICE_DATE",
                    "sl.LINE_ACTUAL_SHIP_DATE",
                ]
            )
        }} as text(255)
    ) as unique_key,
    cast(substring(sl.source_system, 1, 255) as text(255)) as source_system,
    cast(substring(sl.sales_line_number, 1, 255) as text(255)) as sales_line_number,
    cast(substring(sl.sales_order_number, 1, 255) as text(255)) as sales_order_number,
    cast(
        substring(sl.source_sales_order_type, 1, 255) as text(255)
    ) as source_sales_order_type,
    cast(substring(sl.sales_order_type, 1, 10) as text(10)) as sales_order_type,
    cast(substring(sl.sales_order_company, 1, 10) as text(10)) as sales_order_company,
    cast(sl.line_invoice_date as timestamp_ntz(9)) as line_invoice_date,
    cast(sl.line_actual_ship_date as timestamp_ntz(9)) as line_actual_ship_date,
    cast(
        substring(sl.source_item_identifier, 1, 255) as text(255)
    ) as source_item_identifier,
    cast(sl.item_guid as text(255)) as item_guid,
    cast(sl.variant_code as text(255)) as variant_code,
    cast(
        substring(sl.ship_source_customer_code, 1, 255) as text(255)
    ) as ship_source_customer_code,
    cast(sl.ship_customer_address_guid as text(255)) as ship_customer_address_guid,
    'CA' as trans_uom,
    'GBP' as base_currency,
    cast(
        first_value(cbom.raw_materials) over (
            partition by
                sl.sales_line_number,
                upper(cbom.root_company_code),
                upper(cbom.root_src_item_identifier)
            order by cbom.eff_date desc
        ) as number(38, 10)
    ) as base_ext_ing_cost,
    cast(
        first_value(cbom.packaging) over (
            partition by
                sl.sales_line_number,
                upper(cbom.root_company_code),
                upper(cbom.root_src_item_identifier)
            order by cbom.eff_date desc
        ) as number(38, 10)
    ) as base_ext_pkg_cost,
    cast(
        first_value(cbom.labour) over (
            partition by
                sl.sales_line_number,
                upper(cbom.root_company_code),
                upper(cbom.root_src_item_identifier)
            order by cbom.eff_date desc
        ) as number(38, 10)
    ) as base_ext_lbr_cost,
    cast(
        first_value(cbom.bought_in) over (
            partition by
                sl.sales_line_number,
                upper(cbom.root_company_code),
                upper(cbom.root_src_item_identifier)
            order by cbom.eff_date desc
        ) as number(38, 10)
    ) as base_ext_bought_in_cost,
    cast(
        first_value(cbom.other) over (
            partition by
                sl.sales_line_number,
                upper(cbom.root_company_code),
                upper(cbom.root_src_item_identifier)
            order by cbom.eff_date desc
        ) as number(38, 10)
    ) as base_ext_oth_cost,
    cast(
        first_value(cbom.co_pack) over (
            partition by
                sl.sales_line_number,
                upper(cbom.root_company_code),
                upper(cbom.root_src_item_identifier)
            order by cbom.eff_date desc
        ) as number(38, 10)
    ) as base_ext_copack_cost,
    cast(current_timestamp as timestamp_ntz(9)) as load_date
from slsorder sl
left outer join
    cbom
    on upper(cbom.root_company_code) = upper(sl.sales_order_company)
    and upper(sl.source_item_identifier) = upper(cbom.root_src_item_identifier)
    and (
        cbom.variant_flag = 0
        or (
            cbom.variant_flag = 1
            and upper(sl.variant_code) = upper(cbom.root_src_variant_code)
        )
    )
where cbom.eff_date <= sl.source_updated_time
