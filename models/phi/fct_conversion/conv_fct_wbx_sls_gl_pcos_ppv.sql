{{
    config(
    enabled=true,
    materialized = env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_fact","ax_hist_sales"]
    )
}}

/*Adding below variables for mapping account id with account category */

{% set list_PCOS_VAR_FG =  "('521020','521025','521040','521045','521046','521047','521070','540005','540010','540020','540025','550010','550030','550040','560010','560020','560030','560032','560033','560034')" %}
{% set list_PCOS_VAR_OTHER =  "('510030','521010','521011','521050','531045','540030')" %}
{% set list_PCOS_VAR_PACKAGIN =  "('521030','521035','540015','550015','560031')" %}
{% set list_PCOS_VAR_COPACK =  "('521015','531040','531050')" %}
{% set list_PCOS_VAR_LABOUR =  "('510040','510045','710010','710015','710020','710025','710030','710035','710040','710045','710050','710051','710052','710055','710060','710065','710070','710090','737040')" %}

with old_fct as (
    select *
    from {{ source("WBX_PROD_FACT", "fct_wbx_sls_gl_pcos_ppv") }}
    where
        {{ env_var("DBT_PICK_FROM_CONV") }} = 'Y'
        and document_company in ('WBX', 'RFL')
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
        account_guid,
        account_guid_old,
        source_object_id,
        source_object_id_old,
        source_account_identifier,
        source_account_identifier_old
    from {{ ref('conv_dim_wbx_account') }}
),

converted_fct as (
    select
        source_system,
        target_account_identifier,  --check of its new value is needed.conv_dim_wbx_account don't contain this field.
        acnt.account_guid as account_guid,
        a.source_object_id as source_object_id_old,
        acnt.source_object_id as source_object_id,
        a.business_unit_address_guid as business_unit_address_guid_old,
        plnt.plantdc_address_guid_new as business_unit_address_guid,
        journal_entry_flag,
        gl_date,
        document_company,
        a.source_account_identifier as source_account_identifier_old,
        acnt.source_account_identifier as source_account_identifier,
        a.source_business_unit_code as source_business_unit_code_old,
        plnt.source_business_unit_code_new as source_business_unit_code,
        base_currency,
        transaction_currency,
        phi_currency,
        pcomp_currency,
        base_ledger_amt,
        txn_ledger_amt,
        phi_ledger_amt,
        pcomp_ledger_amt,
        source_item_identifier,
        item_guid,
       -- ship_source_customer_code,  
       -- bill_source_customer_code, 
       -- ship_customer_addr_number_guid, 
       -- bill_customer_addr_number_guid, 
        fiscal_year_period_no,
        trade_type,
        product_class,
        --journal_entry_type,
        union_logic,
        unique_key,
        'AX' as source_legacy,
        load_date,
        update_date
    from old_fct as a
    left join
        old_plant as plnt
        on a.business_unit_address_guid = plnt.plantdc_address_guid
    left join old_account as acnt on a.account_guid = acnt.account_guid_old
),

unique_key_regen as(
    select 
    source_system,
        target_account_identifier,
        account_guid,
        source_object_id_old,
        source_object_id,
        case 
            when source_object_id_old in {{list_PCOS_VAR_FG}} then 'PCOS VAR - FG'
            when source_object_id_old in {{list_PCOS_VAR_OTHER}} then 'PCOS VAR - OTHER'
            when source_object_id_old in {{list_PCOS_VAR_PACKAGIN}} then 'PCOS VAR - PACKAGIN'
            when source_object_id_old in {{list_PCOS_VAR_COPACK}} then 'PCOS VAR - COPACK' 
            when source_object_id_old in {{list_PCOS_VAR_LABOUR}} then 'PCOS VAR - LABOUR'
        end as account_category, 
        business_unit_address_guid_old,
        business_unit_address_guid,
        journal_entry_flag,
        gl_date,
        document_company,
        source_account_identifier_old,
        source_account_identifier,
        source_business_unit_code_old,
        source_business_unit_code,
        base_currency,
        transaction_currency,
        phi_currency,
        pcomp_currency,
        base_ledger_amt,
        txn_ledger_amt,
        phi_ledger_amt,
        pcomp_ledger_amt,
        source_item_identifier,
        item_guid, 
        fiscal_year_period_no,
        trade_type,
        product_class,
        union_logic,
        unique_key,
        source_legacy,
        load_date,
        update_date,
        cast(
        {{
            dbt_utils.surrogate_key(
                [
                    "SOURCE_ITEM_IDENTIFIER",
                    "SOURCE_BUSINESS_UNIT_CODE",
                    "TARGET_ACCOUNT_IDENTIFIER",
                    "TRANSACTION_CURRENCY",
                    "DOCUMENT_COMPANY",
                    "UNION_LOGIC",
                    "GL_DATE",
                    "SOURCE_OBJECT_ID",
                    "JOURNAL_ENTRY_FLAG",
                    "TRADE_TYPE"
                ]
            )
        }} as text(255)
        ) as unique_key_new
       from converted_fct

)

select
    cast(unique_key_new as text(255)) as unique_key,
    cast(unique_key as text(255)) as unique_key_old,
    cast(substring(source_system, 1, 255) as text(255)) as source_system,
    cast(substring(target_account_identifier, 1, 255) as text(255))
        as target_account_identifier,
    cast(account_guid as text(255)) as account_guid,
    cast(substring(source_object_id_old, 1, 255) as text(255))
        as source_object_id_old,
    cast(substring(source_object_id, 1, 255) as text(255)) as source_object_id,
    cast(substring(account_category, 1, 255) as text(255)) as account_category,
    cast(business_unit_address_guid_old as text(255))
        as business_unit_address_guid_old,
    cast(business_unit_address_guid as text(255)) as business_unit_address_guid,
    cast(journal_entry_flag as text(10)) as journal_entry_flag,
    cast(gl_date as date) as gl_date,
    cast(substring(document_company, 1, 20) as text(20)) as document_company,
    cast(substring(source_account_identifier_old, 1, 255) as text(255))
        as source_account_identifier_old,
    cast(substring(source_account_identifier, 1, 255) as text(255))
        as source_account_identifier,
    cast(substring(source_business_unit_code_old, 1, 255) as text(255))
        as source_business_unit_code_old,
    cast(substring(source_business_unit_code, 1, 255) as text(255))
        as source_business_unit_code,
    cast(substring(base_currency, 1, 20) as text(20)) as base_currency,
    cast(substring(transaction_currency, 1, 20) as text(20))
        as transaction_currency,
    cast(substring(phi_currency, 1, 20) as text(20))
        as phi_currency,
    cast(substring(pcomp_currency, 1, 20) as text(20))
        as pcomp_currency,
    cast(base_ledger_amt as number(38, 10)) as base_ledger_amt,
    cast(txn_ledger_amt as number(38, 10)) as txn_ledger_amt,
    cast(phi_ledger_amt as number(38, 10)) as phi_ledger_amt,
    cast(pcomp_ledger_amt as number(38, 10)) as pcomp_ledger_amt,
    cast(substring(source_item_identifier, 1, 255) as text(255))
        as source_item_identifier,
    cast(substring(item_guid, 1, 255) as text(255))
        as item_guid,
    cast(substring(fiscal_year_period_no, 1, 255) as text(255))
        as fiscal_year_period_no,
    cast(trade_type as text(10)) as trade_type,
    cast(product_class as text(10)) as product_class,
    cast(substring(union_logic, 1, 255) as text(255))
        as union_logic,
    cast(source_legacy as text(50)) as source_legacy,
    cast(load_date as date) as load_date,
    cast(update_date as date) as update_date
from unique_key_regen