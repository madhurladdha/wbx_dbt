

with source as (

    select * from {{ source('SHAREPOINT_DSCI', 'ctp_plant_wc_target') }}

),

plant_cross_ref as (select * from {{ ref("plant_d365_ref") }}),

renamed as (

    select
        _line,
        cast(substring(nvl(plant_ref.d365, source.plant),1,255) as text(255)) as plant,
        work_center_name,
        ctp_target,
        ptp_target,
        _fivetran_synced

    from source left join
        plant_cross_ref as plant_ref
        on upper(trim(source.plant)) = upper(trim(plant_ref.ax))

)

select * from renamed
