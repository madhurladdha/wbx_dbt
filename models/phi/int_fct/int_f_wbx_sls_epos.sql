{{ config(tags=["sales", "epos","sls_epos"]) }}

with
    stg_f_wbx_sls_epos as (select * from {{ ref("stg_f_wbx_sls_epos") }}),
    xref_wbx_sls_customer_pushdown as (
        select * from {{ ref("xref_wbx_sls_pushdown_customer") }}
    ),
    dim_wbx_item as (select * from {{ ref("dim_wbx_item") }}),
    dim_wbx_date_oc as (select * from {{ ref("dim_wbx_date_oc") }}),
    --One calendar_year_week_no can have more than one fiscal_period_no
    --The below code will pick one fiscal_period_no same as that in IICS
    wbx_date_oc_fiscal_period as (
        select
            *,
            row_number() over (
                partition by calendar_year_week_no
                order by day_of_month_business desc, day_of_month_actual
            ) rownum
        from dim_wbx_date_oc
        qualify rownum = 1
    ),
    dim_wbx_uom as (select * from {{ ref("dim_wbx_uom") }}),
    cal_cnt as (
        select count(distinct calendar_date) as number_of_days, calendar_year_week_no
        from dim_wbx_date_oc
        group by calendar_year_week_no
    ),
    source as (
        select
            {{
                dbt_utils.surrogate_key(
                    [
                        "sls_wtx_epos_stg.source_system",
                        "sls_wtx_epos_stg.source_item_identifier",
                        "xref_wbx_sls_customer_pushdown.bill_source_customer_code",
                        "dim_date_oc.calendar_date",
                    ]
                )
            }} as unique_key,
            sls_wtx_epos_stg.source_system,
            dim_wbx_item.item_guid,
            trade_type,
            sls_wtx_epos_stg.source_item_identifier,
            xref_wbx_sls_customer_pushdown.bill_source_customer_code,
            xref_wbx_sls_customer_pushdown.bill_customer_address_guid,
            calendar_week,
            wbx_date_oc_fiscal_period.fiscal_period_no,
            qty_ca,
            dim_date_oc.calendar_date,
            number_of_days,
            qty_ca / number_of_days as o_qty_ca,
            qty_ca / number_of_days * uom_factor_ca_kg.conversion_rate as qty_kg,
            qty_ca / number_of_days * uom_factor_ca_pl.conversion_rate as qty_ul,
            qty_ca
            / number_of_days
            * case
                when dim_wbx_item.primary_uom = 'CA'
                then 1
                when dim_wbx_item.primary_uom = 'KG'
                then uom_factor_ca_kg.conversion_rate
                when dim_wbx_item.primary_uom = 'PL'
                then uom_factor_ca_pl.conversion_rate
                when var1.conversion_rate is not null
                then var1.conversion_rate
                when var1.conversion_rate is null
                then 0.00
            end as qty_prim,
            dim_wbx_item.primary_uom
        from stg_f_wbx_sls_epos sls_wtx_epos_stg
        inner join
            dim_wbx_date_oc dim_date_oc
            on sls_wtx_epos_stg.calendar_week = dim_date_oc.calendar_year_week_no
        join
            cal_cnt on cal_cnt.calendar_year_week_no = dim_date_oc.calendar_year_week_no
        left join
            xref_wbx_sls_customer_pushdown
            on sls_wtx_epos_stg.source_system
            = xref_wbx_sls_customer_pushdown.source_system
            and sls_wtx_epos_stg.trade_type
            = xref_wbx_sls_customer_pushdown.trade_type_code
        left join
            dim_wbx_item
            on sls_wtx_epos_stg.source_system = dim_wbx_item.source_system
            and sls_wtx_epos_stg.source_item_identifier
            = dim_wbx_item.source_item_identifier
        left join
            wbx_date_oc_fiscal_period
            on sls_wtx_epos_stg.calendar_week
            = wbx_date_oc_fiscal_period.calendar_year_week_no
        left join
            {{
                ent_dbt_package.lkp_uom(
                    "dim_wbx_item.item_guid",
                    "'CA'",
                    "'PL'",
                    "UOM_FACTOR_CA_PL",
                )
            }}
        left join
            {{
                ent_dbt_package.lkp_uom(
                    "dim_wbx_item.item_guid", "'CA'", "'KG'", "UOM_FACTOR_CA_KG"
                )
            }}
        left join
            {{
                ent_dbt_package.lkp_uom(
                    "dim_wbx_item.item_guid",
                    "'CA'",
                    "dim_wbx_item.primary_uom",
                    "var1",
                )
            }}
        where dim_wbx_item.item_guid is not null
    )

select *
from source
