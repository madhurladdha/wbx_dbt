{{
    config(
        tags=["ax_hist_fact","ax_hist_sales","ax_hist_on_demand"]
    )
}}



with source as (

    select * from {{ source('FACTS_FOR_COMPARE', 'sls_wtx_budget_terms_fact') }} where  {{env_var("DBT_PICK_FROM_CONV")}}='Y'

),

renamed as (

    select
        source_system,
        cust_idx,
        plan_source_customer_code,
        customer_address_number_guid,
        sku_idx,
        source_item_identifier,
        cast({{ dbt_utils.surrogate_key(['source_system','source_item_identifier']) }} as text(255)) as item_guid,
        calendar_date,
        snapshot_date,
        term_code,
        term_desc,
        term_create_datetime,
        term_created_by,
        scen_idx,
        scen_name,
        scen_code,
        cast({{ dbt_utils.surrogate_key(["source_system", "scen_idx"]) }} as text(255)) as scenario_guid,
        frozen_forecast,
        rsa_perc,
        lump_sum,
        perc_invoiced_sales,
        perc_gross_sales,
        early_settlement_perc,
        edlp_perc,
        edlp_case_rate,
        long_term_promo,
        rsi_perc,
        fixed_annual_payment,
        direct_shopper_marketing,
        other_direct_payment,
        other_direct_perc,
        category_payment,
        indirect_shopper_marketing,
        other_indirect_payment,
        other_indirect_perc,
        field_marketing,
        consumer_spend,
        term_start_date,
        term_end_date,
        status_code,
        status_name,
        status_verb,
        impact_option_code,
        impact_option_name,
        impact_code,
        impact_name,
        impact_option_valvol_percent,
        impact_option_lump_sum_flag,
        impact_option_value,
        impact_option_fin_impact_estimate,
        sls_wtx_budget_terms_fact_skey

    from source

)

select * from renamed

