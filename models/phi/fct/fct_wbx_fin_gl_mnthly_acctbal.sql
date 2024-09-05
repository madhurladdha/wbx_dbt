{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        tags=["finance","gl","gl_monthly"],
        unique_key="unique_key",
        snowflake_warehouse=env_var('DBT_WBX_SF_WH'),
        on_schema_change="sync_all_columns",
        incremental_strategy="delete+insert",
        pre_hook="""
            {% set now = modules.datetime.datetime.now() %}
            {%- set full_load_day -%} {{env_var('DBT_FULL_LOAD_DAY')}} {%- endset -%}
            {%- set day_today -%} {{ now.strftime('%A') }} {%- endset -%}
            {% if day_today == full_load_day %}
            {{ truncate_if_exists(this.schema, this.table) }}
            {% endif %}
            """,
    )
}}

/* The GL Monthly Balance Fact, as part of the D365 conversion from AX now combines (unions) the Converted AX History data w/ the "new" data from D365 to make
    up the full historical set for the given Fact.
    If a given unique row (primary key) exists in both sets that means it was converted by the D365 team in the transaction system.  In such cases
    the code here is meant to ensure that only the D365 row is pulled through and materialized.
*/


with int_fact as 
(
select * from {{ ref("int_f_wbx_fin_gl_mnthly_acctbal") }} qualify row_number() over (partition by unique_key order by 1)=1
),

old_ax_fact as
(
select * from {{ref('conv_fct_wbx_fin_gl_mnthly_acctbal')}}
),

int as(
    select
        unique_key,
        source_system,
        source_account_identifier,
        target_account_identifier,
        source_object_id,
        source_subsidiary_id,
        source_company_code,
        account_guid,
        company_code,
        fiscal_period_number,
        source_business_unit_code,
        business_unit_address_guid,
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
        'd365' as source_legacy
    from int_fact
),

Ax_hist as
(
select
a.unique_key as unique_key,
a.source_system as source_system,
a.source_account_identifier as source_account_identifier,
a.target_account_identifier as target_account_identifier,
a.source_object_id as source_object_id,
a.source_subsidiary_id as source_subsidiary_id,
a.source_company_code as source_company_code,
a.account_guid as account_guid,
a.company_code as company_code,
a.fiscal_period_number as fiscal_period_number,
a.source_business_unit_code as source_business_unit_code,
a.business_unit_address_guid as business_unit_address_guid,
a.ledger_type as ledger_type,
a.txn_currency as txn_currency,
a.base_currency as base_currency,
a.phi_currency as phi_currency,
a.pcomp_currency as pcomp_currency,
a.txn_conv_rt as txn_conv_rt,
a.base_conv_rt as base_conv_rt,
a.phi_conv_rt as phi_conv_rt,
a.pcomp_conv_rt as pcomp_conv_rt,
a.txn_prior_year_ending_bal as txn_prior_year_ending_bal,
a.txn_period_change_amt as txn_period_change_amt,
a.txn_ytd_bal as txn_ytd_bal,
a.base_prior_year_ending_bal as base_prior_year_ending_bal,
a.base_period_change_amt as base_period_change_amt,
a.base_ytd_bal as base_ytd_bal,
a.phi_prior_year_ending_bal as phi_prior_year_ending_bal,
a.phi_period_change_amt as phi_period_change_amt,
a.phi_ytd_bal as phi_ytd_bal,
a.pcomp_prior_year_ending_bal as pcomp_prior_year_ending_bal,
a.pcomp_period_change_amt as pcomp_period_change_amt,
a.pcomp_ytd_bal as pcomp_ytd_bal,
a.source_date_updated as source_date_updated,
a.load_date as load_date,
a.update_date as update_date,
a.source_updated_d_id as source_updated_d_id,
a.source_legacy as source_legacy
/* Only want the AX History data IF the unique key is NOT in the D365 data from the int model.
*/
from old_ax_fact a
left join int b on a.unique_key=b.unique_key where b.source_system is null
),

Final as
(
select * from int
union
select * from ax_hist
)

select
cast(unique_key as text(255) )                                      as unique_key,
cast(substring(source_system,1,255) as text(255) )                  as source_system,
cast(substring(source_account_identifier,1,255) as text(255) )      as source_account_identifier,
cast(substring(target_account_identifier,1,255) as text(255) )      as target_account_identifier,
cast(substring(source_object_id,1,255) as text(255) )               as source_object_id,
cast(substring(source_subsidiary_id,1,255) as text(255) )            as source_subsidary_id,
cast(substring(source_company_code,1,255) as text(255) )            as source_company_code,
cast(account_guid as text(255) )                                    as account_guid,
cast(substring(company_code,1,20) as text(20) )                     as company_code,
cast(fiscal_period_number as number(38,0) )                         as fiscal_period_number,
cast(substring(source_business_unit_code,1,255) as text(255) )      as source_business_unit_code,
cast(business_unit_address_guid as text(255) )                      as business_unit_address_guid,
cast(substring(ledger_type,1,255) as text(255) )                    as ledger_type,
cast(substring(txn_currency,1,20) as text(20) )                     as txn_currency,
cast(substring(base_currency,1,20) as text(20) )                    as base_currency,
cast(substring(phi_currency,1,20) as text(20) )                     as phi_currency,
cast(substring(pcomp_currency,1,20) as text(20) )                   as pcomp_currency,
cast(txn_conv_rt as number(29,9) )                                  as txn_conv_rt,
cast(base_conv_rt as number(29,9) )                                 as base_conv_rt,
cast(phi_conv_rt as number(29,9) )                                  as phi_conv_rt,
cast(pcomp_conv_rt as number(29,9) )                                as pcomp_conv_rt,
cast(txn_prior_year_ending_bal as number(38,10) )                   as txn_prior_year_ending_bal,
cast(txn_period_change_amt as number(38,10) )                       as txn_period_change_amt,
cast(txn_ytd_bal as number(38,10) )                                 as txn_ytd_bal,
cast(base_prior_year_ending_bal as number(38,10) )                  as base_prior_year_ending_bal,
cast(base_period_change_amt as number(38,10) )                      as base_period_change_amt,
cast(base_ytd_bal as number(38,10) )                                as base_ytd_bal,
cast(phi_prior_year_ending_bal as number(38,10) )                   as phi_prior_year_ending_bal,
cast(phi_period_change_amt as number(38,10) )                       as phi_period_change_amt,
cast(phi_ytd_bal as number(38,10) )                                 as phi_ytd_bal,
cast(pcomp_prior_year_ending_bal as number(38,10) )                 as pcomp_prior_year_ending_bal,
cast(pcomp_period_change_amt as number(38,10) )                     as pcomp_period_change_amt,
cast(pcomp_ytd_bal as number(38,10) )                               as pcomp_ytd_bal,
cast(source_date_updated as timestamp_ntz(9) )                      as source_date_updated,
cast(load_date as timestamp_ntz(9) )                                as load_date,
cast(update_date as timestamp_ntz(9) )                              as update_date,
cast(source_updated_d_id as number(38,0) )                          as source_updated_d_id,
cast(source_legacy as text(15))                                      as  source_legacy
from Final