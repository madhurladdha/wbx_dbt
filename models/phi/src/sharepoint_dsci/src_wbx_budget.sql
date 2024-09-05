

with source as (

    select * from {{ source('SHAREPOINT_DSCI', 'wtx_budget_sheet_1') }}

),

renamed as (

    select
        _line,
        jul,
        oct,
        item_code,
        feb,
        apr,
        jun,
        dec,
        may,
        item_id,
        scenario,
        aug,
        year,
        nov,
        jan,
        company,
        mar,
        sep,
        _fivetran_synced,
        aug_eur,
        dec_eur,
        oct_pound,
        apr_eur,
        mar_eur,
        jan_eur,
        feb_eur,
        may_eur,
        jul_eur,
        oct_eur,
        sep_eur,
        jun_eur,
        nov_eur,
        may_pound,
        apr_pound,
        sep_pound,
        jan_pound,
        aug_pound,
        jul_pound,
        nov_pound,
        jun_pound,
        feb_pound,
        mar_pound,
        dec_pound

    from source

)

select * from renamed

