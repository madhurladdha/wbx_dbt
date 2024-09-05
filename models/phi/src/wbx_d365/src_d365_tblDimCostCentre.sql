

with source as (

    select * from {{ source('WEETABIX', 'DBIX_tblDimCostCentre') }}

),

renamed as (

    select
        kcostcentre,
        dcc_costcentre,
        dcc_description,
        dcc_costcentredesc,
        dcc_mktcde,
        dcc_submktcde,
        dcc_brdcd

    from source

)

select * from renamed
