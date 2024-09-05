

with source as (

    select * from {{ source('EI_RDM', 'sls_wtx_mape_targets') }}

),

renamed as (

    select
        trade_type_code,
        tgt_mape,
        tgt_bias,
        active_flag,
        eff_date,
        expir_date

    from source

)

select * from renamed

