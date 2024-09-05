/*  29-May-2023: repointing the main source from EI_RDM.ref_effective_currency_dim to DIM_ENT.v_dim_effective_currency
*/

with source as (

    select * from {{ source('DIM_ENT', 'v_dim_effective_currency') }}
    where source_system ='{{env_var("DBT_SOURCE_SYSTEM")}}'

),

renamed as (

    select
        source_system,
        source_business_unit_code,
        plant_dc_address_guid,
        company_code,
        company_address_guid,
        plant_default_currency_code,
        company_default_currency_code,
        parent_currency_code,
        effective_date,
        expiration_date,
        null as load_date,
        null as update_date

    from source

)

select * from renamed
