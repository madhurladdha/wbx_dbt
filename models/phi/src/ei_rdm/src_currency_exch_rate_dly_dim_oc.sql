

with source as (

    select * from {{ source('EI_RDM', 'currency_exch_rate_dly_dim_oc') }}
    where source_system ='{{env_var("DBT_SOURCE_SYSTEM")}}'
),

renamed as (

    select
        curr_from_code,
        curr_to_code,
        curr_conv_rt,
        curr_conv_rate_i,
        eff_d_id,
        eff_from_d,
        expir_d_id,
        expir_to_d,
        rollfwd_row,
        source_system

    from source

)

select * from renamed