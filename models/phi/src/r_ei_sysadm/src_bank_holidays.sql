with source as (

    select * from {{ source('R_EI_SYSADM', 'bank_holidays') }}

),

renamed as (

    select
YEAR,
DATE,
DAY_OF_WEEK,
DESCRIPTION
    from source

)

select * from renamed
