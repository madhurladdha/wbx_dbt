{{ config(
    snowflake_warehouse=env_var("DBT_WBX_SF_WH"), 
    tags=["sales", "uber"]
    ) 
}}
with slsuber_fact as (
    select * from {{ ref("fct_wbx_sls_uber") }}
),
customer_ext as (
    select * from {{ ref("dim_wbx_customer_ext") }}
),
fc_snapshot as (
    select * from {{ ref('dim_wbx_fc_snapshot')}}
),
lkp_snapshot_date as (
    --changed from src_sls_wtx_lkp_snapshot_date to stg_d_wtx_lkp_snapshot_date
    select * from {{ ref('stg_d_wtx_lkp_snapshot_date')}}
),
dim_date as (
    select * from {{ ref('src_dim_date')}}
),
item_ext as (
    select
        source_system,
        source_item_identifier,
        max(dummy_product_flag) as dummy_product_flag,
        max(item_type) as item_type,
        max(branding_desc) as branding_desc,
        max(product_class_desc) as product_class_desc,
        max(sub_product_desc) as sub_product_desc,
        max(strategic_desc) as strategic_desc,
        max(power_brand_desc) as power_brand_desc,
        max(manufacturing_group_desc) as manufacturing_group_desc,
        max(category_desc) as category_desc,
        max(pack_size_desc) as pack_size_desc,
        max(sub_category_desc) as sub_category_desc,
        max(consumer_units_in_trade_units) as consumer_units_in_trade_units,
        max(promo_type_desc) as promo_type_desc,
        max(consumer_units) as consumer_units,
        max(description) as description
    from {{ ref('dim_wbx_item_ext')}}
    group by source_system, source_item_identifier
),
planning_date_oc as (
    select * from {{ ref('dim_wbx_planning_date_oc')}}
),
customer_planning as (
    select * from {{ ref('stg_d_wbx_customer_planning')}}
),
combo_min_date as (
        select
            ub.source_item_identifier as source_item_identifier,
            coalesce(cust_ext.trade_type_code, plan.trade_type_code) as trade_type_code,
            min(ub.calendar_date) as calendar_date
        from slsuber_fact ub
        left join
            customer_ext cust_ext
            on --ub.source_system = cust_ext.source_system
             ub.customer_addr_number_guid = cust_ext.customer_address_number_guid
           -- and nvl(ub.document_company, 'WBX') = cust_ext.company_code
        left join
            customer_planning plan
            on trim(ub.plan_source_customer_code) = trim(plan.trade_type_code)
        where
            source_content_filter in ('ACTUALS', 'FORECAST')
            and calendar_date
            >= date_trunc('MONTH', dateadd('MONTH', -24, current_date))
            and calendar_date <= date_trunc('MONTH', dateadd('MONTH', 18, current_date))
            and (ub.snapshot_forecast_date in (
                 select snapshot_date
                 from (select
                          snapshot_date,
                          rank() over (
                                    partition by source_system, snapshot_type
                                    order by snapshot_date desc
                                ) rank_no
                            from fc_snapshot
                            where snapshot_type = 'MONTH_END'
                                and snapshot_date <= (
                                    select snapshot_date
                                    from lkp_snapshot_date))
                    where rank_no <= 3)
                or ub.snapshot_forecast_date
                = (select snapshot_date from lkp_snapshot_date)
                or ub.snapshot_forecast_date is null)
            and (cy_ordered_ca_quantity <> 0
                or cy_ordered_kg_quantity <> 0
                or cy_shipped_ca_quantity <> 0
                or cy_shipped_kg_quantity <> 0
                or ub.fcf_tot_vol_kg <> 0
                or ub.fcf_tot_vol_ca <> 0) group by 1, 2
    ),

    zero_data_set as (
        select distinct
            dd.calendar_date as calendar_date,
            ub.source_system as source_system,
            ub.source_item_identifier as source_item_identifier,
            0 as item_guid,
            coalesce(cust_ext.trade_type_code, plan.trade_type_code) as trade_type_code,
            coalesce(cust_ext.trade_type_desc, plan.trade_type) as trade_type_desc
        from slsuber_fact ub
        inner join
            dim_date dd
            on 1 = 1
            and dd.calendar_date
            >= date_trunc('MONTH', dateadd('MONTH', -24, current_date))
            and dd.calendar_date
            <= date_trunc('MONTH', dateadd('MONTH', 18, current_date))
        left join
            customer_ext cust_ext
            on  ub.customer_addr_number_guid = cust_ext.customer_address_number_guid
        left join customer_planning plan
            on trim(ub.plan_source_customer_code) = trim(plan.trade_type_code)
        where
            source_content_filter in ('ACTUALS', 'FORECAST')
            and (
                ub.snapshot_forecast_date in (
                    select snapshot_date
                    from
                        (
                            select
                                snapshot_date,
                                rank() over (
                                    partition by source_system, snapshot_type
                                    order by snapshot_date desc
                                ) rank_no
                            from fc_snapshot
                            where
                                snapshot_type = 'MONTH_END'
                                and snapshot_date <= (
                                    select snapshot_date
                                    from lkp_snapshot_date
                                )
                        )
                    where rank_no <= 3
                )
                or ub.snapshot_forecast_date
                = (select snapshot_date from lkp_snapshot_date)
                or ub.snapshot_forecast_date is null
            )

    ),
    actuals_data_set as (
        select
            ub.snapshot_forecast_date as snapshot_forecast_date, 
            ub.calendar_date as calendar_date,
            ub.source_system as source_system,
            ub.source_item_identifier as source_item_identifier,
            0 as item_guid, 
            cust_ext.trade_type_code as trade_type_code,
            cust_ext.trade_type_desc as trade_type_desc,
            ub.cy_confirmdate_original as original_order_date,  -- - 
            coalesce(
                ub.trans_line_requested_date,
                ub.cy_scheduled_ship_date,
                ub.calendar_date
            ) as original_requested_ship_dt,
            sum(cy_ordered_ca_quantity) as ordered_ca_quantity,
            sum(cy_ordered_kg_quantity) as ordered_kg_quantity,
            sum(cy_shipped_ca_quantity) as shipped_ca_quantity,
            sum(cy_shipped_kg_quantity) as shipped_kg_quantity,
            sum(cy_ca_quantity_original) as original_ca_quantity,
            sum(cy_kg_quantity_original) as original_kg_quantity
        from slsuber_fact ub
        left join
            customer_ext cust_ext
            on ub.customer_addr_number_guid = cust_ext.customer_address_number_guid
        where
            source_content_filter = 'ACTUALS'
            and calendar_date
            >= date_trunc('MONTH', dateadd('MONTH', -24, current_date))
            and calendar_date
            <= (select snapshot_date from lkp_snapshot_date)
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9
    ),

    forecast_live as (
        select
            ub.snapshot_forecast_date as snapshot_forecast_date,
            ub.calendar_date as calendar_date,
            ub.source_system as source_system,
            ub.source_item_identifier as source_item_identifier,
            0 as item_guid,  
            plan.trade_type_code as trade_type_code,
            plan.trade_type as trade_type_desc,
            sum(nvl(ub.fcf_tot_vol_kg, 0)) as fcf_tot_vol_kg, 
            sum(nvl(ub.fcf_tot_vol_ca, 0)) as fcf_tot_vol_ca 
        from slsuber_fact ub
        left join
            customer_planning plan
            on trim(ub.plan_source_customer_code) = trim(plan.trade_type_code)
        where
            source_content_filter = 'FORECAST'
            and calendar_date
            >= date_trunc('MONTH', dateadd('MONTH', -24, current_date))
            and calendar_date <= date_trunc('MONTH', dateadd('MONTH', 18, current_date))
            and ub.snapshot_forecast_date
            = (select snapshot_date from lkp_snapshot_date)
        group by 1, 2, 3, 4, 5, 6, 7
    ),
    forecast_m1 as (
        select
            ub.snapshot_forecast_date as snapshot_forecast_date,
            ub.calendar_date as calendar_date,
            ub.source_system as source_system,
            ub.source_item_identifier as source_item_identifier,
            0 as item_guid,  
            plan.trade_type_code as trade_type_code,
            plan.trade_type as trade_type_desc,
            sum(nvl(ub.fcf_tot_vol_kg, 0)) as fcf_tot_vol_kg,
            sum(nvl(ub.fcf_tot_vol_ca, 0)) as fcf_tot_vol_ca 
        from slsuber_fact ub
        left join
            customer_planning plan
            on trim(ub.plan_source_customer_code) = trim(plan.trade_type_code)
        where
            source_content_filter = 'FORECAST'
            and calendar_date
            >= date_trunc('MONTH', dateadd('MONTH', -24, current_date))
            and calendar_date <= date_trunc('MONTH', dateadd('MONTH', 18, current_date))
            and ub.snapshot_forecast_date = (
                select snapshot_date
                from
                    (select
                            snapshot_date,
                            rank() over (
                                partition by source_system, snapshot_type
                                order by snapshot_date desc
                            ) rank_no
                        from fc_snapshot
                        where
                            snapshot_type = 'MONTH_END'
                            and snapshot_date <= (
                                select snapshot_date
                                from lkp_snapshot_date
                            )
                    )
                where rank_no = 1
            )
        group by 1, 2, 3, 4, 5, 6, 7
    ),
    forecast_m2 as (
        select
            ub.snapshot_forecast_date as snapshot_forecast_date,
            ub.calendar_date as calendar_date,
            ub.source_system as source_system,
            ub.source_item_identifier as source_item_identifier,
            0 as item_guid,
            plan.trade_type_code as trade_type_code,
            plan.trade_type as trade_type_desc,
            sum(nvl(ub.fcf_tot_vol_kg, 0)) as fcf_tot_vol_kg, 
            sum(nvl(ub.fcf_tot_vol_ca, 0)) as fcf_tot_vol_ca 
        from slsuber_fact ub
        left join
            customer_planning plan
            on trim(ub.plan_source_customer_code) = trim(plan.trade_type_code)
        where
            source_content_filter = 'FORECAST'
            and calendar_date
            >= date_trunc('MONTH', dateadd('MONTH', -24, current_date))
            and calendar_date <= date_trunc('MONTH', dateadd('MONTH', 18, current_date))
            and ub.snapshot_forecast_date = (
                select snapshot_date
                from
                    (
                        select
                            snapshot_date,
                            rank() over (
                                partition by source_system, snapshot_type
                                order by snapshot_date desc
                            ) rank_no
                        from fc_snapshot
                        where
                            snapshot_type = 'MONTH_END'
                            and snapshot_date <= (
                                select snapshot_date
                                from lkp_snapshot_date
                            )
                    )
                where rank_no = 2
            )
        group by 1, 2, 3, 4, 5, 6, 7
    ),
    forecast_m3 as (
        select
            ub.snapshot_forecast_date as snapshot_forecast_date,
            ub.calendar_date as calendar_date,
            ub.source_system as source_system,
            ub.source_item_identifier as source_item_identifier,
            0 as item_guid, 
            plan.trade_type_code as trade_type_code,
            plan.trade_type as trade_type_desc,
            sum(nvl(ub.fcf_tot_vol_kg, 0)) as fcf_tot_vol_kg,
            sum(nvl(ub.fcf_tot_vol_ca, 0)) as fcf_tot_vol_ca 
        from slsuber_fact ub
        left join
            customer_planning plan
            on trim(ub.plan_source_customer_code) = trim(plan.trade_type_code)
        where
            source_content_filter = 'FORECAST'
            and calendar_date
            >= date_trunc('MONTH', dateadd('MONTH', -24, current_date))
            and calendar_date <= date_trunc('MONTH', dateadd('MONTH', 18, current_date))
            and ub.snapshot_forecast_date = (
                select snapshot_date
                from
                    (
                        select
                            snapshot_date,
                            rank() over (
                                partition by source_system, snapshot_type
                                order by snapshot_date desc
                            ) rank_no
                        from fc_snapshot
                        where
                            snapshot_type = 'MONTH_END'
                            and snapshot_date <= (
                                select snapshot_date
                                from lkp_snapshot_date
                            )
                    )
                where rank_no = 3
            )
        group by 1, 2, 3, 4, 5, 6, 7
    ),

    tfm as (
        select
            zds.source_system as source_system,
            zds.calendar_date as calendar_date,
            zds.source_item_identifier as source_item_identifier,
            zds.item_guid as item_guid,
            zds.trade_type_code as trade_type_code,
            f_live.snapshot_forecast_date as f_live_snapshot_date,
            f_m1.snapshot_forecast_date as f_m1_snapshot_date,
            f_m2.snapshot_forecast_date as f_m2_snapshot_date,
            f_m3.snapshot_forecast_date as f_m3_snapshot_date,
            cmd.calendar_date as combo_min_date,
            ads.original_order_date as original_order_date,
            ads.original_requested_ship_dt as original_requested_ship_dt,
            sum(nvl(ads.ordered_ca_quantity, 0)) as ordered_ca_quantity,
            sum(nvl(ads.ordered_kg_quantity, 0)) as ordered_kg_quantity,
            sum(nvl(ads.shipped_ca_quantity, 0)) as shipped_ca_quantity,
            sum(nvl(ads.shipped_kg_quantity, 0)) as shipped_kg_quantity,
            sum(nvl(ads.original_ca_quantity, 0)) as original_ca_quantity,
            sum(nvl(ads.original_kg_quantity, 0)) as original_kg_quantity,
            sum(nvl(f_live.fcf_tot_vol_kg, 0)) as f_live_tot_vol_kg,
            sum(nvl(f_live.fcf_tot_vol_ca, 0)) as f_live_tot_vol_ca,
            sum(nvl(f_m1.fcf_tot_vol_kg, 0)) as f_m1_tot_vol_kg,
            sum(nvl(f_m1.fcf_tot_vol_ca, 0)) as f_m1_tot_vol_ca,
            sum(nvl(f_m2.fcf_tot_vol_kg, 0)) as f_m2_tot_vol_kg,
            sum(nvl(f_m2.fcf_tot_vol_ca, 0)) as f_m2_tot_vol_ca,
            sum(nvl(f_m3.fcf_tot_vol_kg, 0)) as f_m3_tot_vol_kg,
            sum(nvl(f_m3.fcf_tot_vol_ca, 0)) as f_m3_tot_vol_ca
        from zero_data_set zds
        inner join
            combo_min_date cmd
            on zds.source_item_identifier = cmd.source_item_identifier
            and zds.trade_type_code = cmd.trade_type_code
            and zds.calendar_date >= cmd.calendar_date 
        left outer join
            actuals_data_set ads
            on zds.calendar_date = ads.calendar_date
            and zds.source_system = ads.source_system
            and zds.source_item_identifier = ads.source_item_identifier
            and zds.trade_type_code = ads.trade_type_code
        left outer join
            forecast_live f_live
            on zds.calendar_date = f_live.calendar_date
            and zds.source_system = f_live.source_system
            and zds.source_item_identifier = f_live.source_item_identifier
            and zds.trade_type_code = f_live.trade_type_code
        left outer join
            forecast_m1 f_m1
            on zds.calendar_date = f_m1.calendar_date
            and zds.source_system = f_m1.source_system
            and zds.source_item_identifier = f_m1.source_item_identifier
            and zds.trade_type_code = f_m1.trade_type_code
        left outer join
            forecast_m2 f_m2
            on zds.calendar_date = f_m2.calendar_date
            and zds.source_system = f_m2.source_system
            and zds.source_item_identifier = f_m2.source_item_identifier
            and zds.trade_type_code = f_m2.trade_type_code
        left outer join
            forecast_m3 f_m3
            on zds.calendar_date = f_m3.calendar_date
            and zds.source_system = f_m3.source_system
            and zds.source_item_identifier = f_m3.source_item_identifier
            and zds.trade_type_code = f_m3.trade_type_code
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    ),
final as (
select
    ub.source_system as source_system,
    ub.calendar_date as calendar_date,
    ub.source_item_identifier as source_item_identifier,
    ub.item_guid as item_guid,
    ub.trade_type_code as trade_type_code,
    ub.f_live_snapshot_date as f_live_snapshot_date,
    ub.f_m1_snapshot_date as f_m1_snapshot_date,
    ub.f_m2_snapshot_date as f_m2_snapshot_date,
    ub.f_m3_snapshot_date as f_m3_snapshot_date,
    ub.combo_min_date as combo_min_date,
    ub.original_order_date as original_order_date,
    ub.original_requested_ship_dt as original_requested_ship_dt,
    ub.ordered_ca_quantity as ordered_ca_quantity,
    ub.ordered_kg_quantity as ordered_kg_quantity,
    ub.shipped_ca_quantity as shipped_ca_quantity,
    ub.shipped_kg_quantity as shipped_kg_quantity,
    ub.original_ca_quantity as original_ca_quantity,
    ub.original_kg_quantity as original_kg_quantity,
    ub.f_live_tot_vol_kg as f_live_tot_vol_kg,
    ub.f_live_tot_vol_ca as f_live_tot_vol_ca,
    ub.f_m1_tot_vol_kg as f_m1_tot_vol_kg,
    ub.f_m1_tot_vol_ca as f_m1_tot_vol_ca,
    ub.f_m2_tot_vol_kg as f_m2_tot_vol_kg,
    ub.f_m2_tot_vol_ca as f_m2_tot_vol_ca,
    ub.f_m3_tot_vol_kg as f_m3_tot_vol_kg,
    ub.f_m3_tot_vol_ca as f_m3_tot_vol_ca,
    dt.report_fiscal_year,
    dt.report_fiscal_year_period_no,
    dt.fiscal_year_begin_dt,
    dt.fiscal_year_end_dt,
    dtp.planning_week_code,
    dtp.planning_week_start_dt,
    dtp.planning_week_end_dt,
    dtp.planning_week_no,
    dtp.planning_month_code,
    dtp.planning_month_start_dt,
    dtp.planning_month_end_dt,
    dtp.planning_quarter_no,
    dtp.planning_quarter_start_dt,
    dtp.planning_quarter_end_dt,
    dtp.planning_year_no,
    dtp.planning_year_start_dt,
    dtp.planning_year_end_dt,
    plan.market as market,
    plan.sub_market as submarket,
    plan.trade_class as trade_class,
    plan.trade_group as trade_group,
    plan.trade_type as trade_type,
    plan.trade_sector_desc as trade_sector,
    itm_ext.description,
    itm_ext.item_type,
    itm_ext.branding_desc,
    itm_ext.product_class_desc,
    itm_ext.sub_product_desc,
    itm_ext.strategic_desc,
    itm_ext.power_brand_desc,
    itm_ext.manufacturing_group_desc,
    itm_ext.category_desc,
    itm_ext.pack_size_desc,
    itm_ext.sub_category_desc,
    itm_ext.promo_type_desc,
    nvl(itm_ext.dummy_product_flag, 0) as dummy_product_flag,
    dtp_or.planning_week_code as or_planning_week_code,
    dtp_or.planning_week_start_dt as or_planning_week_start_dt,
    dtp_or.planning_week_end_dt as or_planning_week_end_dt,
    dtp_or.planning_week_no as or_planning_week_no,
    dtp_or.planning_month_code as or_planning_month_code,
    dtp_or.planning_month_start_dt as or_planning_month_start_dt,
    dtp_or.planning_month_end_dt as or_planning_month_end_dt,
    dtp_or.planning_quarter_no as or_planning_quarter_no,
    dtp_or.planning_quarter_start_dt as or_planning_quarter_start_dt,
    dtp_or.planning_quarter_end_dt as or_planning_quarter_end_dt,
    dtp_or.planning_year_no as or_planning_year_no,
    dtp_or.planning_year_start_dt as or_planning_year_start_dt,
    dtp_or.planning_year_end_dt as or_planning_year_end_dt
from tfm ub
left join dim_date dt on ub.calendar_date = dt.calendar_date
left join
    planning_date_oc dtp
    on ub.source_system = dtp.source_system
    and ub.calendar_date = dtp.calendar_date
left join
    planning_date_oc dtp_or
    on ub.source_system = dtp_or.source_system
    and ub.original_requested_ship_dt = dtp_or.calendar_date
left join item_ext itm_ext
    on ub.source_system = itm_ext.source_system
    and ub.source_item_identifier = itm_ext.source_item_identifier
left join
    customer_planning plan
    on trim(ub.trade_type_code) = trim(plan.trade_type_code)
)
select 
    source_system,
    calendar_date,
    source_item_identifier,
    item_guid,
    trade_type_code,
    f_live_snapshot_date,
    f_m1_snapshot_date,
    f_m2_snapshot_date,
    f_m3_snapshot_date,
    combo_min_date,
    original_order_date,
    original_requested_ship_dt,
    ordered_ca_quantity,
    ordered_kg_quantity,
    shipped_ca_quantity,
    shipped_kg_quantity,
    original_ca_quantity,
    original_kg_quantity,
    f_live_tot_vol_kg,
    f_live_tot_vol_ca,
    f_m1_tot_vol_kg,
    f_m1_tot_vol_ca,
    f_m2_tot_vol_kg,
    f_m2_tot_vol_ca,
    f_m3_tot_vol_kg,
    f_m3_tot_vol_ca,
    report_fiscal_year,
    report_fiscal_year_period_no,
    fiscal_year_begin_dt,
    fiscal_year_end_dt,
    planning_week_code,
    planning_week_start_dt,
    planning_week_end_dt,
    planning_week_no,
    planning_month_code,
    planning_month_start_dt,
    planning_month_end_dt,
    planning_quarter_no,
    planning_quarter_start_dt,
    planning_quarter_end_dt,
    planning_year_no,
    planning_year_start_dt,
    planning_year_end_dt,
    market,
    submarket,
    trade_class,
    trade_group,
    trade_type,
    trade_sector,
    description,
    item_type,
    branding_desc,
    product_class_desc,
    sub_product_desc,
    strategic_desc,
    power_brand_desc,
    manufacturing_group_desc,
    category_desc,
    pack_size_desc,
    sub_category_desc,
    promo_type_desc,
    dummy_product_flag,
    or_planning_week_code,
    or_planning_week_start_dt,
    or_planning_week_end_dt,
    or_planning_week_no,
    or_planning_month_code,
    or_planning_month_start_dt,
    or_planning_month_end_dt,
    or_planning_quarter_no,
    or_planning_quarter_start_dt,
    or_planning_quarter_end_dt,
    or_planning_year_no,
    or_planning_year_start_dt,
    or_planning_year_end_dt
from final
