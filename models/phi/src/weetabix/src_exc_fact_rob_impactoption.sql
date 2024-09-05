

with source as (

    select * from {{ source('WEETABIX', 'EXC_Fact_ROB_ImpactOption') }}

),

renamed as (

    select
        rob_idx,
        impactoption_idx,
        value,
        financialimpactestimate

    from source

)

select * from renamed
