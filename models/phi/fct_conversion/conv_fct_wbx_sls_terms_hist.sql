{{
    config(
        tags=["ax_hist_fact","ax_hist_sales","ax_hist_on_demand"],
        snowflake_warehouse= env_var("DBT_WBX_SF_WH")
    )
}}
/*  This conversion model has specific filters added to it so as to limit the volume that needs to be copied over to the dbt model.
    Weetabix does not require every snapshot going back through time and the intent is to clean-up the snapshots soon anyway.
    This filter is designed to include only the following, and set the guids.
        -Any snapshot designated as a weekend in the FC Snapshot Dim
        -Every day's snapshot for the last 31 days
        -Any Saturday Snapshots that precede what is set up in FC Snapshot Dim.
*/
WITH 
old_fct AS (
SELECT * FROM {{source('FACTS_FOR_COMPARE','sls_wtx_terms_hist')}}  WHERE SOURCE_SYSTEM = '{{env_var("DBT_SOURCE_SYSTEM")}}' and  {{env_var("DBT_PICK_FROM_CONV")}}='Y'
),

get_snaps as 
(select * from {{ref('dim_wbx_fc_snapshot')}} where snapshot_type = 'WEEK_END'
),

filter_to_weekends as (
select a.* from old_fct a inner join get_snaps b on a.snapshot_date = b.snapshot_date
    where 
        b.snapshot_date is not null or 
        a.snapshot_date >= current_date - 31 or
        DAYOFWEEK(a.snapshot_date) = 6 ---Saturdays
    /*  filters so that only snapshots are copied from the last 31 days OR those designated as weekends by the FC Snapshot dimension.   */
),

converted_fct as (
    select 
        source_system,
        cust_idx,
        plan_source_customer_code,
        customer_address_number_guid,
        sku_idx,
        source_item_identifier,
        {{dbt_utils.surrogate_key(["source_system","source_item_identifier"])}}  as item_guid,
        calendar_date,
        snapshot_date,
        term_code,
        term_desc,
        term_create_datetime,
        term_created_by,
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
        {{ dbt_utils.surrogate_key(["source_system","plan_source_customer_code","source_item_identifier","term_code","calendar_date","snapshot_date"]) }} as unique_key
      
    from filter_to_weekends
)

select * from converted_fct 