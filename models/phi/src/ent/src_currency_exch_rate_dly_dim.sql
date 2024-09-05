/*  29-May-2023: repointing the main source from EI_RDM.currency_exch_rate_dly_dim to DIM_ENT.dim_exchange_rate_dly
*/

with source as (

    select * from {{ source('DIM_ENT', 'dim_exchange_rate_dly') }}

),

renamed as (

    select
        curr_from_code,
        curr_to_code,
        curr_conv_rt,
        curr_conv_rt_i,  --changed from curr_conv_rate_i
        eff_d_id,
        eff_from_d,
        expir_d_id,
        expir_to_d,
        rollfwd_row

    from source

)

select * from renamed