{{
    config(
    enabled=true,
    materialized = env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_fact","ax_hist_finance"]
    )
}}

/* For GL Monthly Balances of AX History, the following dimension fields are possible for "conversion".
    -Cost Center (aka Plant Dim): source_business_unit_code and plantdc_address_guid
    -Account Dim: source_object_id, account_guid, source_account_identifier

    Since the Account Dim is converted and source_account_identifier is part of the unique_key generation, we have to recreate the new unique_key in this code.

    Specific to GL related tables, a filter is added on company_code to ensure that we only pull over WBX and RFL.  These are the only companies of interest
    and this simplifies the data set.  The old historical data from AX for other companies would always still remain in WBX_PROD.FACT.FCT_WBX_FIN_GL_MNTHLY_ACCTBAL.
*/


with old_fct as (
    select *
    from {{ source("WBX_PROD_FACT", "fct_wbx_fin_gl_mnthly_acctbal") }}
    where {{ env_var("DBT_PICK_FROM_CONV") }} = 'Y'
    and company_code in ('WBX','RFL')
),

old_plant as (
    select
        source_business_unit_code_new,
        source_business_unit_code,
        plantdc_address_guid_new,
        plantdc_address_guid
    from {{ ref('conv_dim_wbx_plant_dc') }}
),

old_account as (
    select
        source_object_id,
        source_object_id_old,
        account_guid,
        account_guid_old,
        source_account_identifier,
        source_account_identifier_old
    from {{ ref('conv_dim_wbx_account') }}
),

converted_fct as (
    select
        source_system,
        a.source_account_identifier as source_account_identifier_old,
        acnt.source_account_identifier as source_account_identifier,
        target_account_identifier,
        a.source_object_id as source_object_id_old,
        acnt.source_object_id as source_object_id,
        source_subsidary_id,
        source_company_code,
        a.account_guid as account_guid_old,
        acnt.account_guid as account_guid,
        company_code,
        fiscal_period_number,
        a.source_business_unit_code as source_business_unit_code_old,
        plnt.source_business_unit_code_new as source_business_unit_code,
        a.business_unit_address_guid as business_unit_address_guid_old,
        plnt.plantdc_address_guid_new as business_unit_address_guid,
        ledger_type,
        txn_currency,
        base_currency,
        phi_currency,
        pcomp_currency,
        txn_conv_rt,
        base_conv_rt,
        phi_conv_rt,
        pcomp_conv_rt,
        txn_prior_year_ending_bal,
        txn_period_change_amt,
        txn_ytd_bal,
        base_prior_year_ending_bal,
        base_period_change_amt,
        base_ytd_bal,
        phi_prior_year_ending_bal,
        phi_period_change_amt,
        phi_ytd_bal,
        pcomp_prior_year_ending_bal,
        pcomp_period_change_amt,
        pcomp_ytd_bal,
        source_date_updated,
        load_date,
        update_date,
        source_updated_d_id,
        unique_key,
        'AX' as source_legacy

    from old_fct as a
    left join
        old_plant as plnt
        on a.business_unit_address_guid = plnt.plantdc_address_guid
    left join old_account as acnt on a.account_guid = acnt.account_guid_old
),

/* As one of the key values, source_account_identifier, has changed in this conversion we have to regenerate the unique_key based on that.
*/
unique_key_regen as 
(
    select *,
        {{ dbt_utils.surrogate_key(
            [
                "source_system",
                "source_account_identifier",
                "source_business_unit_code",
                "company_code",
                "fiscal_period_number",
                "txn_currency",
                "ledger_type",
            ]
            )
        }} as unique_key_new
    from converted_fct
)

select
    cast(unique_key_new as text(255)) as unique_key,
    cast(unique_key as text(255)) as unique_key_old,
    cast(substring(source_system, 1, 255) as text(255)) as source_system,
    cast(substring(source_account_identifier_old, 1, 255) as text(255))
        as source_account_identifier_old,
    cast(substring(source_account_identifier, 1, 255) as text(255))
        as source_account_identifier,
    cast(substring(target_account_identifier, 1, 255) as text(255))
        as target_account_identifier,
    cast(substring(source_object_id_old, 1, 255) as text(255))
        as source_object_id_old,
    cast(substring(source_object_id, 1, 255) as text(255)) as source_object_id,
    cast(substring(source_subsidary_id, 1, 255) as text(255))
        as source_subsidiary_id,
    cast(substring(source_company_code, 1, 255) as text(255))
        as source_company_code,
    cast(account_guid_old as text(255)) as account_guid_old,
    cast(account_guid as text(255)) as account_guid,
    cast(substring(company_code, 1, 20) as text(20)) as company_code,
    cast(fiscal_period_number as number(38, 0)) as fiscal_period_number,
    cast(substring(source_business_unit_code_old, 1, 255) as text(255))
        as source_business_unit_code_old,
    cast(substring(source_business_unit_code, 1, 255) as text(255))
        as source_business_unit_code,
    cast(business_unit_address_guid_old as text(255))
        as business_unit_address_guid_old,
    cast(business_unit_address_guid as text(255)) as business_unit_address_guid,
    cast(substring(ledger_type, 1, 255) as text(255)) as ledger_type,
    cast(substring(txn_currency, 1, 20) as text(20)) as txn_currency,
    cast(substring(base_currency, 1, 20) as text(20)) as base_currency,
    cast(substring(phi_currency, 1, 20) as text(20)) as phi_currency,
    cast(substring(pcomp_currency, 1, 20) as text(20)) as pcomp_currency,
    cast(txn_conv_rt as number(29, 9)) as txn_conv_rt,
    cast(base_conv_rt as number(29, 9)) as base_conv_rt,
    cast(phi_conv_rt as number(29, 9)) as phi_conv_rt,
    cast(pcomp_conv_rt as number(29, 9)) as pcomp_conv_rt,
    cast(txn_prior_year_ending_bal as number(38, 10))
        as txn_prior_year_ending_bal,
    cast(txn_period_change_amt as number(38, 10)) as txn_period_change_amt,
    cast(txn_ytd_bal as number(38, 10)) as txn_ytd_bal,
    cast(base_prior_year_ending_bal as number(38, 10))
        as base_prior_year_ending_bal,
    cast(base_period_change_amt as number(38, 10)) as base_period_change_amt,
    cast(base_ytd_bal as number(38, 10)) as base_ytd_bal,
    cast(phi_prior_year_ending_bal as number(38, 10))
        as phi_prior_year_ending_bal,
    cast(phi_period_change_amt as number(38, 10)) as phi_period_change_amt,
    cast(phi_ytd_bal as number(38, 10)) as phi_ytd_bal,
    cast(pcomp_prior_year_ending_bal as number(38, 10))
        as pcomp_prior_year_ending_bal,
    cast(pcomp_period_change_amt as number(38, 10)) as pcomp_period_change_amt,
    cast(pcomp_ytd_bal as number(38, 10)) as pcomp_ytd_bal,
    cast(source_date_updated as timestamp_ntz(9)) as source_date_updated,
    cast(load_date as timestamp_ntz(9)) as load_date,
    cast(update_date as timestamp_ntz(9)) as update_date,
    cast(source_updated_d_id as number(38, 0)) as source_updated_d_id,
    cast(source_legacy as text(15)) as source_legacy
from unique_key_regen


