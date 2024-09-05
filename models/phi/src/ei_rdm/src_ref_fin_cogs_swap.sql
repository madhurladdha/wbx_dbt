with source as (

    select * from {{ source('EI_RDM', 'ref_fin_cogs_swap') }}

),

renamed as (

    select
        target_cc,
        account_prepend

    from source

)

select * from renamed