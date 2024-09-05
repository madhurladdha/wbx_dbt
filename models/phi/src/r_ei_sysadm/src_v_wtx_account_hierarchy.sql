

with source as (

    select * from {{ source('FACTS_FOR_COMPARE', 'v_wtx_account_hierarchy') }}

),

renamed as (

    select
        node_level,
        market_code,
        market,
        sub_market_code,
        sub_market,
        trade_class_code,
        trade_class,
        trade_group_code,
        trade_group,
        trade_type_code,
        trade_type,
        customer_account_number,
        customer_account,
        customer_branch_number,
        customer_branch,
        trade_sector_code,
        trade_sector_desc,
        market_seq,
        sub_market_seq,
        trade_class_seq,
        trade_group_seq,
        trade_type_seq,
        trade_sector_seq,
        price_group,
        total_so_qty_discount,
        additional_discount,
        customer_rebate_group,
        customer,
        customer_desc,
        company_code,
        company_name,
        customer_name,
        customer_type,
        currency_code,
        unified_customer,
        customer_group,
        customer_group_name,
        csr_name,
        long_address_number,
        source_name,
        tax_number,
        address_line_1,
        postal_code,
        city,
        county,
        state_province,
        country_code,
        country,
        source,
        obsolete

    from source

)

select * from renamed

