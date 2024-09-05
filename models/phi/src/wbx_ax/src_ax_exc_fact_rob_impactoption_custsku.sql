

with source as (

    select * from {{ source('WEETABIX', 'EXC_Fact_ROB_ImpactOption_CustSku') }}

),

renamed as (

    select
        rob_idx,
        cust_idx,
        sku_idx,
        impactoption_idx,
        date_start_idx,
        date_end_idx,
        value

    from source

)

select * from renamed
