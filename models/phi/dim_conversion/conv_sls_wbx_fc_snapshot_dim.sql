

with source as (

    select * from {{ source('EI_RDM', 'sls_wtx_fc_snapshot_dim') }} where {{env_var("DBT_PICK_FROM_CONV")}}='Y' /*adding variable to include/exclude conversion model data.if variable DBT_PICK_FROM_CONV has value 'Y' then conversion model will pull data from hist else it will be null */

),

renamed as (

    select
        source_system,
        snapshot_date,
        snapshot_model,
        snapshot_code,
        snapshot_type,
        snapshot_desc,
        concensus_flag,
        purge_date,
        load_date,
        update_date

    from source

)

select * from renamed
