

with source as (

    select * from {{ source('WEETABIX', 'EXC_Fact_ROB_LumpSum_Spread_CustSkuDay') }}

),

renamed as (

    select
        rob_idx,
        cust_idx,
        sku_idx,
        day_idx,
        scen_idx,
        impactoption_idx,
        rob_value,
        resultantvalue

    from source

)

select * from renamed
