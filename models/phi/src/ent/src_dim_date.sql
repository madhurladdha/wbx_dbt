with source as (

    select * from {{ source('DIM_ENT', 'dim_date') }}

),

renamed as (

    select
        fiscal_date_id,
        fiscal_period_no,
        fiscal_year_period_no,
        fiscal_period_desc,
        fiscal_period_begin_dt,
        fiscal_period_end_dt,
        fiscal_year_quarter_no,
        fiscal_quarter_desc,
        fiscal_quarter_start_dt,
        fiscal_quarter_end_dt,
        fiscal_year,
        fiscal_year_begin_dt,
        fiscal_year_end_dt,
        null as fiscal_year_week_no,
        calendar_date_id,
        calendar_date,
        calendar_day_of_week,
        calendar_year,
        calendar_year_begin_dt,
        calendar_year_end_dt,
        calendar_year_quarter_no,
        calendar_quarter_desc,
        calendar_quarter_start_dt,
        calendar_quarter_end_dt,
        calendar_year_month_no,
        calendar_month_no,
        calendar_month_name,
        calendar_month_desc,
        calendar_month_start_dt,
        calendar_month_end_dt,
        null as calendar_year_week_no,
        calendar_week_begin_dt,
        calendar_week_end_dt,
        calendar_business_day_flag,
        source_ind,
        load_date,
        update_date,
        report_fiscal_year_period_no,
        report_fiscal_year

    from source

)

select * from renamed
