

with source as (

    select * from {{ source('FACTS_FOR_COMPARE', 'v_wtx_cust_planning') }}

),

renamed as (

    select
        market_code,
        market,
        market_code_seq,
        sub_market_code,
        sub_market,
        sub_market_code_seq,
        trade_class_code,
        trade_class,
        trade_class_seq,
        trade_group_code,
        trade_group,
        trade_group_seq,
        trade_type_code,
        trade_type,
        trade_type_seq,
        trade_sector_code,
        trade_sector_desc,
        trade_sector_seq

    from source

)

select * from renamed

