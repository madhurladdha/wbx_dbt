

with source as (

    select * from {{ source('R_EI_SYSADM', 'sls_wtx_lkp_snapshot_date') }}

),

renamed as (

    select
        snapshot_date

    from source

)

select * from renamed
