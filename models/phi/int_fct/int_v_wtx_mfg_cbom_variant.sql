{{
  config( 
    materialized=env_var('DBT_MAT_INCREMENTAL'), 
    tags=["manufacturing", "cbom","mfg_cbom","sales","cogs","pcos"],
    pre_hook="""
    {% if check_table_exists( this.schema, this.table ) == 'True' %}
        truncate table {{ this }}
    {% endif %}  """,
    )
}}

/* Model is downstream of fct_wbx_mfg_cbom and preps the data to be used downstream in Sales Order related models.  This is used there to 
    help calculate the Standard PCOS numbers by multiplying the given item's cost against he Cases Shipped in Sales Orders.  This particular model
    is specific to sales orders which is why it pulls in the intermediate model to form the outgoing structure of this model around that key.
    
    The CBOM will need to be handled differently depending on the Company in question (currently WBX, IBE, or RFL).
    -   Iberica (IBE) does not have the cost break down through the use of Cost Groups in AX/D365 the way the other companies can.  IBE only has a "BoughtIn" cost.
        The code will need to reflect that.

    Key Information by Company that should result and may drive the calcs.

    Iberica (IBE):
        -Company Code field itself will be IBE.
        -Currency Code aligns w/ BASE, expected to be in Euro (€)
        -Module Type (0=Inventory)

    Ryecroft (RFL):
        -Company Code field itself will be RFL.
        -Currency Code aligns w/ BASE, expected to be in GBP (£)
        -Module Type (0=Inventory)

    Weetabix UK (WBX):
        -Company Code field itself will be WBX.
        -Currency Code aligns w/ BASE, expected to be in GBP (£)
        -Module Type (0=Inventory)

    The model includes some hard-coded aspects around the Site of WBX-CBY.  This does not seem to impact the IBE results.
*/

with fct_wbx_mfg_cbom as (
    select * from {{ ref('fct_wbx_mfg_cbom') }}
),
int_f_wbx_sls_order as (
    select * from {{ ref('int_f_wbx_sls_order') }}
),
dim_wbx_item_ext as (
    select * from {{ ref('dim_wbx_item_ext') }}
),
dim_wbx_item as (
    select * from {{ ref('dim_wbx_item') }}
),
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
                order by eff_date desc
                range between current row and unbounded following
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
                    from fct_wbx_mfg_cbom bom
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
                            from fct_wbx_mfg_cbom f1
                            inner join
                                fct_wbx_mfg_cbom f2
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
                            from dim_wbx_item im
                            left outer join
                                dim_wbx_item_ext e
                                on im.item_guid = e.item_guid
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
                /* For D365 and the introduction of Iberica(IBE) for this model, the following case statement
                    assumed that if the comp_cost_group_id is '' and the level is zero, then it defaults to BI (BoughtIn).
                    That is the expected behavior for IBE at this time as it should all fall to BoughtIn and is how the costing 
                    in D365 is currently set up.
                */
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
sl.source_system,
sl.sales_line_number,
sl.sales_order_number,
sl.source_sales_order_type,
sl.sales_order_type,
sl.sales_order_company,
sl.line_invoice_date,
sl.line_actual_ship_date,
sl.source_item_identifier,
sl.item_guid,
variant_code,
sl.ship_source_customer_code,
sl.ship_customer_address_guid,
/* These 2 fields are not required as output for this model.  If required downstream, then the base uom and currency code 
    should be identified and used there, aligned with the relevant Company Code's defaults.
*/
-- 'CA' as trans_uom,
-- 'GBP' as base_currency,
first_value(cbom.raw_materials) over (
    partition by
        sl.sales_line_number,
        upper(cbom.root_company_code),
        upper(cbom.root_src_item_identifier)
    order by cbom.eff_date desc
) as base_ext_ing_cost,
first_value(cbom.packaging) over (
    partition by
        sl.sales_line_number,
        upper(cbom.root_company_code),
        upper(cbom.root_src_item_identifier)
    order by cbom.eff_date desc
) as base_ext_pkg_cost,
first_value(cbom.labour) over (
    partition by
        sl.sales_line_number,
        upper(cbom.root_company_code),
        upper(cbom.root_src_item_identifier)
    order by cbom.eff_date desc
) as base_ext_lbr_cost,
first_value(cbom.bought_in) over (
    partition by
        sl.sales_line_number,
        upper(cbom.root_company_code),
        upper(cbom.root_src_item_identifier)
    order by cbom.eff_date desc
) as base_ext_bought_in_cost,
first_value(cbom.other) over (
    partition by
        sl.sales_line_number,
        upper(cbom.root_company_code),
        upper(cbom.root_src_item_identifier)
    order by cbom.eff_date desc
) as base_ext_oth_cost,
first_value(cbom.co_pack) over (
    partition by
        sl.sales_line_number,
        upper(cbom.root_company_code),
        upper(cbom.root_src_item_identifier)
    order by cbom.eff_date desc
) as base_ext_copack_cost
-- ,SL.SHIPPED_CA_QUANTITY
from int_f_wbx_sls_order sl
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
where
cbom.eff_date <= sl.source_updated_time


