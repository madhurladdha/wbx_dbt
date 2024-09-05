{{ config(tags=["sales", "budget", "sls_budget", "sls_budget_fin","adhoc"]) }}

with
    stg as (select * from {{ ref("stg_f_wbx_sls_budget_fin") }}),
    item_cte as (
        select distinct source_system, source_item_identifier, item_guid, primary_uom
        from {{ ref("dim_wbx_item") }}
    ),
    item_ext_cte as (
        select distinct source_system, source_item_identifier, product_class_code
        from {{ ref("dim_wbx_item_ext") }}
    ),
    dim_date_cte as (select * from {{ ref("src_dim_date") }}),
    uom as (
        select source_system, item_guid, from_uom, to_uom, conversion_rate
        from {{ ref("dim_wbx_uom") }}
    ),
    final as (
        select
            cast(substring(source_system, 1, 10) as text(10)) as source_system,
            cast(
                substring(product_class_code, 1, 60) as text(60)
            ) as product_class_code,
            cast(
                substring(source_item_identifier, 1, 60) as text(60)
            ) as source_item_identifier,
            cast(item_guid as text(255)) as item_guid,
            cast(substring(trade_type_code, 1, 60) as text(60)) as trade_type_code,
            cast(
                substring(ship_source_customer_code, 1, 255) as text(255)
            ) as ship_source_customer_code,
            cast(ship_customer_address_guid as text(255)) as ship_customer_address_guid,
            cast(
                substring(bill_source_customer_code, 1, 255) as text(255)
            ) as bill_source_customer_code,
            cast(bill_customer_address_guid as text(255)) as bill_customer_address_guid,
            cast(calendar_date as timestamp_ntz(9)) as calendar_date,
            cast(substring(calendar_month, 1, 255) as text(255)) as calendar_month,
            cast(
                substring(fiscal_period_number, 1, 255) as text(255)
            ) as fiscal_period_number,
            cast(substring(frozen_forecast, 1, 10) as text(10)) as frozen_forecast,
            cast(sum(budget_qty_ca) as number(38, 10)) as budget_qty_ca,
            cast(sum(budget_qty_kg) as number(38, 10)) as budget_qty_kg,
            cast(sum(budget_qty_ul) as number(38, 10)) as budget_qty_ul,
            cast(sum(budget_qty_prim) as number(38, 10)) as budget_qty_prim,
            cast(substring(primary_uom, 1, 255) as text(255)) as primary_uom,
            cast(sum(avp_qty_kg) as number(38, 10)) as avp_qty_kg,
            cast(sum(bars_qty_kg) as number(38, 10)) as bars_qty_kg,
            cast(substring(currency_code, 1, 255) as text(255)) as currency_code,
            cast(sum(waste_reduced_amt) as number(38, 10)) as waste_reduced_amt,
            cast(sum(cleaning_amt) as number(38, 10)) as cleaning_amt,
            cast(sum(engineer_amt) as number(38, 10)) as engineer_amt,
            cast(sum(labour_adj_amt) as number(38, 10)) as labour_adj_amt,
            cast(sum(gross_value_amt) as number(38, 10)) as gross_value_amt,
            cast(sum(edlp_amt) as number(38, 10)) as edlp_amt,
            cast(sum(rsa_amt) as number(38, 10)) as rsa_amt,
            cast(sum(settlement_amt) as number(38, 10)) as settlement_amt,
            cast(sum(gincent_amt) as number(38, 10)) as gincent_amt,
            cast(sum(incentive_forced_amt) as number(38, 10)) as incentive_forced_amt,
            cast(sum(incentive_addl_amt) as number(38, 10)) as incentive_addl_amt,
            cast(sum(other_amt) as number(38, 10)) as other_amt,
            cast(sum(back_margin_amt) as number(38, 10)) as back_margin_amt,
            cast(sum(net_value_amt) as number(38, 10)) as net_value_amt,
            cast(sum(avp_grossup_amt) as number(38, 10)) as avp_grossup_amt,
            cast(sum(net_value_grossup_amt) as number(38, 10)) as net_value_grossup_amt,
            cast(sum(rawmats_cost_amt) as number(38, 10)) as rawmats_cost_amt,
            cast(sum(pack_cost_amt) as number(38, 10)) as pack_cost_amt,
            cast(sum(labour_cost_amt) as number(38, 10)) as labour_cost_amt,
            cast(sum(boughtin_cost_amt) as number(38, 10)) as boughtin_cost_amt,
            cast(sum(copack_cost_amt) as number(38, 10)) as copack_cost_amt,
            cast(sum(rye_adj_cost_amt) as number(38, 10)) as rye_adj_cost_amt,
            cast(sum(total_cost_amt) as number(38, 10)) as total_cost_amt,
            cast(sum(exp_trade_spend_amt) as number(38, 10)) as exp_trade_spend_amt,
            cast(
                sum(exp_consumer_spend_amt) as number(38, 10)
            ) as exp_consumer_spend_amt,
            cast(sum(pif_isa_amt) as number(38, 10)) as pif_isa_amt,
            cast(sum(pif_trade_amt) as number(38, 10)) as pif_trade_amt,
            cast(sum(pif_trade_oib_amt) as number(38, 10)) as pif_trade_oib_amt,
            cast(sum(pif_trade_red_amt) as number(38, 10)) as pif_trade_red_amt,
            cast(sum(pif_trade_avp_amt) as number(38, 10)) as pif_trade_avp_amt,
            cast(sum(pif_trade_enh_amt) as number(38, 10)) as pif_trade_enh_amt,
            cast(sum(mif_category_amt) as number(38, 10)) as mif_category_amt,
            cast(sum(mif_customer_mktg_amt) as number(38, 10)) as mif_customer_mktg_amt,
            cast(sum(mif_field_mktg_amt) as number(38, 10)) as mif_field_mktg_amt,
            cast(sum(mif_isa_amt) as number(38, 10)) as mif_isa_amt,
            cast(
                sum(mif_range_support_incent_amt) as number(38, 10)
            ) as mif_range_support_incent_amt,
            cast(sum(mif_trade_amt) as number(38, 10)) as mif_trade_amt,
            cast(sum(is_extra_amt) as number(38, 10)) as is_extra_amt,
            cast(load_date as timestamp_ntz(9)) as load_date,
            cast(update_date as timestamp_ntz(9)) as update_date

        from
            -- first part of 3 part Union - data is at the Trade Type and Item level,
            -- so no
            -- allocation.  Just the split from weeks down to days.
            (
                select
                    'WEETABIX' as source_system,
                    item_ext.product_class_code,
                    to_char(
                        lpad(trim(src_bud.comcde5d), 5, 0)
                    ) as source_item_identifier,
                    item_dim.item_guid,
                    src_bud.tratypcde as trade_type_code,
                    null as ship_source_customer_code,
                    null as ship_customer_address_guid,
                    null as bill_source_customer_code,
                    null as bill_customer_address_guid,
                    dim_date.calendar_date as calendar_date,
                    dim_date.calendar_month_no as calendar_month,
                    dim_date.fiscal_year_period_no as fiscal_period_number,
                    src_bud.frozen_forecast,
                    src_bud.budqty / dim_date.split_factor as budget_qty_ca,
                    src_bud.budkgs / dim_date.split_factor as budget_qty_kg,
                    src_bud.budpallets / dim_date.split_factor as budget_qty_ul,
                    case
                        when item_dim.primary_uom = 'CA'
                        then src_bud.budqty / dim_date.split_factor
                        else
                            src_bud.budqty
                            / dim_date.split_factor
                            * nvl(uom_factor.conversion_rate, 0)
                    end as budget_qty_prim,
                    item_dim.primary_uom,
                    src_bud.avpkgs / dim_date.split_factor as avp_qty_kg,
                    src_bud.kgsbars / dim_date.split_factor as bars_qty_kg,
                    'GBP' as currency_code,
                    src_bud.wasteredval / dim_date.split_factor as waste_reduced_amt,
                    src_bud.cleaningval / dim_date.split_factor as cleaning_amt,
                    src_bud.valueeng / dim_date.split_factor as engineer_amt,
                    src_bud.labouradjval / dim_date.split_factor as labour_adj_amt,
                    src_bud.grossvaluetotal / dim_date.split_factor as gross_value_amt,
                    src_bud.edlptotal / dim_date.split_factor as edlp_amt,
                    src_bud.rsatotal / dim_date.split_factor as rsa_amt,
                    src_bud.settlementtotal / dim_date.split_factor as settlement_amt,
                    src_bud.gincenttotal / dim_date.split_factor as gincent_amt,
                    src_bud.incentiveforced
                    / dim_date.split_factor as incentive_forced_amt,
                    src_bud.addincenttotal
                    / dim_date.split_factor as incentive_addl_amt,
                    src_bud.othertotal / dim_date.split_factor as other_amt,
                    src_bud.backmargintotal / dim_date.split_factor as back_margin_amt,
                    src_bud.netvaluetotal / dim_date.split_factor as net_value_amt,
                    src_bud.avpgrossup / dim_date.split_factor as avp_grossup_amt,
                    src_bud.netvalgrossup
                    / dim_date.split_factor as net_value_grossup_amt,
                    src_bud.rawmaterialstotal
                    / dim_date.split_factor as rawmats_cost_amt,
                    src_bud.packagingtotal / dim_date.split_factor as pack_cost_amt,
                    src_bud.labourtotal / dim_date.split_factor as labour_cost_amt,
                    src_bud.boughtintotal / dim_date.split_factor as boughtin_cost_amt,
                    src_bud.copackingtotal / dim_date.split_factor as copack_cost_amt,
                    src_bud.ryeadjtotal / dim_date.split_factor as rye_adj_cost_amt,
                    src_bud.totalcost / dim_date.split_factor as total_cost_amt,
                    src_bud.exptradespend
                    / dim_date.split_factor as exp_trade_spend_amt,
                    src_bud.expconsumerspend
                    / dim_date.split_factor as exp_consumer_spend_amt,
                    src_bud."PIF-ISA" / dim_date.split_factor as pif_isa_amt,
                    src_bud."PIF-TRADE" / dim_date.split_factor as pif_trade_amt,
                    src_bud."PIF-TRADE OIB"
                    / dim_date.split_factor as pif_trade_oib_amt,
                    src_bud."PIF-TRADE RED"
                    / dim_date.split_factor as pif_trade_red_amt,
                    src_bud."PIF-TRADE AVP"
                    / dim_date.split_factor as pif_trade_avp_amt,
                    src_bud."PIF-TRADE ENH"
                    / dim_date.split_factor as pif_trade_enh_amt,
                    src_bud."MIF-CATEGORY" / dim_date.split_factor as mif_category_amt,
                    src_bud."MIF-CUSTOMER MARKETING"
                    / dim_date.split_factor as mif_customer_mktg_amt,
                    src_bud."MIF-FIELD MARKETING"
                    / dim_date.split_factor as mif_field_mktg_amt,
                    src_bud."MIF-ISA" / dim_date.split_factor as mif_isa_amt,
                    src_bud."MIF-RANGE SUPPORT INCENTIVE"
                    / dim_date.split_factor as mif_range_support_incent_amt,
                    src_bud."MIF-TRADE" / dim_date.split_factor as mif_trade_amt,
                    src_bud.isaextra / dim_date.split_factor as is_extra_amt,
                    trunc(current_date, 'DD') as load_date,
                    trunc(current_date, 'DD') as update_date
                from
                    -- Main source table(aggregate amts,qtys by key fields)
                    (
                        select
                            tratypcde,
                            comcde5d,
                            cyr,
                            cyrper,
                            frozen_forecast,
                            sum(budqty) as budqty,
                            sum(budkgs) as budkgs,
                            sum(budpallets) as budpallets,
                            sum(avpkgs) as avpkgs,
                            sum(kgsbars) as kgsbars,
                            sum(wasteredval) as wasteredval,
                            sum(cleaningval) as cleaningval,
                            sum(valueeng) as valueeng,
                            sum(labouradjval) as labouradjval,
                            sum(grossvaluetotal) as grossvaluetotal,
                            sum(edlptotal) as edlptotal,
                            sum(rsatotal) as rsatotal,
                            sum(settlementtotal) as settlementtotal,
                            sum(gincenttotal) as gincenttotal,
                            sum(incentiveforced) as incentiveforced,
                            sum(addincenttotal) as addincenttotal,
                            sum(othertotal) as othertotal,
                            sum(backmargintotal) as backmargintotal,
                            sum(netvaluetotal) as netvaluetotal,
                            sum(avpgrossup) as avpgrossup,
                            sum(netvalgrossup) as netvalgrossup,
                            sum(rawmaterialstotal) as rawmaterialstotal,
                            sum(packagingtotal) as packagingtotal,
                            sum(labourtotal) as labourtotal,
                            sum(boughtintotal) as boughtintotal,
                            sum(copackingtotal) as copackingtotal,
                            sum(ryeadjtotal) as ryeadjtotal,
                            sum(totalcost) as totalcost,
                            sum(exptradespend) as exptradespend,
                            sum(expconsumerspend) as expconsumerspend,
                            sum("PIF-ISA") as "PIF-ISA",
                            sum("PIF-TRADE") as "PIF-TRADE",
                            sum("PIF-TRADE OIB") as "PIF-TRADE OIB",
                            sum("PIF-TRADE RED") as "PIF-TRADE RED",
                            sum("PIF-TRADE AVP") as "PIF-TRADE AVP",
                            sum("PIF-TRADE ENH") as "PIF-TRADE ENH",
                            sum("MIF-CATEGORY") as "MIF-CATEGORY",
                            sum("MIF-CUSTOMER MARKETING") as "MIF-CUSTOMER MARKETING",
                            sum("MIF-FIELD MARKETING") as "MIF-FIELD MARKETING",
                            sum("MIF-ISA") as "MIF-ISA",
                            sum(
                                "MIF-RANGE SUPPORT INCENTIVE"
                            ) as "MIF-RANGE SUPPORT INCENTIVE",
                            sum("MIF-TRADE") as "MIF-TRADE",
                            sum("ISAEXTRA") as "ISAEXTRA"
                        from (select * from stg)
                        group by tratypcde, comcde5d, cyr, cyrper, frozen_forecast
                    ) src_bud

                join
                    -- join with DIM DATE and get CALENDAR DATE to split Monthly Data
                    -- into
                    -- daily
                    (
                        select
                            calendar_date,
                            trunc(calendar_year) || case
                                when calendar_month_no < 10
                                then '0' || calendar_month_no
                                else to_char(calendar_month_no)
                            end as calendar_month_no,
                            day(last_day(calendar_date)) as split_factor,
                            fiscal_year_period_no
                        from dim_date_cte
                    ) dim_date
                    on dim_date.calendar_month_no
                    = cyr
                    || case when cyrper < 10 then '0' || cyrper else to_char(cyrper) end
                -- get PRODUCT CLASS CODE
                left join
                    (
                        select distinct source_item_identifier, product_class_code
                        from item_ext_cte
                    ) item_ext
                    on to_char(lpad(trim(src_bud.comcde5d), 5, 0))
                    = item_ext.source_item_identifier
                -- GET ITEM GUID
                left join
                    (
                        select distinct source_item_identifier, item_guid, primary_uom
                        from item_cte
                        where source_system = 'WEETABIX'
                    ) item_dim
                    on item_dim.source_item_identifier
                    = to_char(lpad(trim(src_bud.comcde5d), 5, 0))

                -- GET UOM CONVERSION RATE from CA to PRIMARY
                left join
                    (
                        select item_guid, from_uom, to_uom, conversion_rate
                        from uom
                        where source_system = 'WEETABIX'
                    ) uom_factor
                    on uom_factor.item_guid = item_dim.item_guid
                    and uom_factor.from_uom = 'CA'
                    and uom_factor.to_uom = item_dim.primary_uom

            )

        -- GROUP BY NON METRIC FIELDS
        group by
            source_system,
            product_class_code,
            source_item_identifier,
            item_guid,
            trade_type_code,
            ship_source_customer_code,
            ship_customer_address_guid,
            bill_source_customer_code,
            bill_customer_address_guid,
            calendar_date,
            calendar_month,
            fiscal_period_number,
            frozen_forecast,
            primary_uom,
            currency_code,
            load_date,
            update_date
    )

select
    *,
    {{
        dbt_utils.surrogate_key(
            [
                "cast(ltrim(rtrim(upper(substring(source_system,1,255)))) as text(255))",
                "cast(ltrim(rtrim(substring(source_item_identifier,1,255))) as text(255))",
                "cast(ltrim(rtrim(substring(FROZEN_FORECAST,1,255))) as text(255))",
                "cast(ltrim(rtrim(substring(TRADE_TYPE_CODE,1,255))) as text(255))",
                "cast(calendar_date as timestamp_ntz(9))",
            ]
        )
    }}
    as unique_key
from final
