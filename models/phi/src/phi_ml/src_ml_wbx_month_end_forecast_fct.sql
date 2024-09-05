

with source as (

    select * from {{ source('PHI_ML', 'ml_wbx_month_end_forecast_fct') }}

),

renamed as (

    select
        ml_cust_dim,
        ml_item_dim,
        ml_dc_dim,
        vdate,
        year_month,
        cum_invoiced_tgt,
        avg_kg_price,
        ytbi_per_day,
        ml_curr_snap_pending,
        days_left,
        weekdays_left,
        avg_time_btwn_orders_last_60,
        avg_lead_time_60,
        days_since_last_order,
        target_movavg_28,
        exp_days_to_close,
        weighted_days_to_close,
        delayed_movavg_56,
        target_movavg_42,
        naive,
        pending_per_day,
        prediction,
        ml_snap_target,
        ytbi_prediction,
        ytbi_day_avg_prediction,
        tgt_per_holiday,
        update_date,
        load_date

    from source

)

select * from renamed

