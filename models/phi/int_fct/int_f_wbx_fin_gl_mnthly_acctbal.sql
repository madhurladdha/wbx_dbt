/* transactional table there is no date column, todo the FULL REFRESH  */
{{
    config(
        on_schema_change="sync_all_columns",
        tags=["finance","gl","gl_monthly"]
       
    )
}}

with source as (
    select * from {{ ref('stg_f_wbx_fin_gl_mnthly_acctbal')}}
),
lkp_account as (
      select
            upper(source_system)        as source_system,
            account_guid                as account_guid,
            source_object_id            as source_object_id,
            source_company_code         as source_company_code,
            source_subsidiary_id        as source_subsidiary_id,
            source_account_identifier   as source_account_identifier,
            tagetik_account             as tagetik_account,
            consolidation_account       as consolidation_account,
            source_business_unit_code   as source_business_unit_code
        from {{ ref("dim_wbx_account") }}
),
dim_date as (
    select  
        report_fiscal_year_period_no as report_fiscal_year_period_no,
        max(calendar_month_end_dt)  as lkp_gl_date
    from {{ ref('src_dim_date')}}
    group by report_fiscal_year_period_no

 --   select * from {{ ref('src_dim_date')}}
),

dim_calender as (
    select * from {{ ref('src_dim_date')}}
),

ref_effective_currency_dim as (
        select 
            source_system,
            company_default_currency_code,
            effective_date,
            expiration_date,
            parent_currency_code,
            {{ dbt_utils.surrogate_key([
           'source_system',
           'company_code',
           "'COMPANY'"]) }} as company_address_guid
    from {{ref('src_ref_effective_currency_dim')}}
),
company_master as (
        select * from {{ ref('dim_wbx_company')}}
),
pcomp_curr as ( 
        select  
            distinct  ltrim(rtrim(b.parent_currency_code)) as parent_currency_code,  
            ltrim(rtrim(a.source_system)) as source_system, 
            ltrim(rtrim(a.company_code)) as company_code,
            b.effective_date as effective_date,
            b.expiration_date as expiration_date 
        from company_master a
        , ref_effective_currency_dim b
        where a.company_address_guid = b.company_address_guid 
        and a.source_system= '{{env_var("DBT_SOURCE_SYSTEM")}}'
        --order by source_system,company_code,effective_date,expiration_date,parent_currency_code
),

base_curr as (

       select 
            distinct ltrim(rtrim(b.company_default_currency_code)) as default_currency_code,
            ltrim(rtrim(a.source_system)) as source_system,
            ltrim(rtrim(a.company_code)) as company_code, 
            b.effective_date as effective_date,
            b.expiration_date as expiration_date
        from company_master a
        , ref_effective_currency_dim b
        where a.company_address_guid = b.company_address_guid
        and a.source_system='{{env_var("DBT_SOURCE_SYSTEM")}}'
),
xref as (
     select * from {{ ref('xref_wbx_hierarchy')}}
),
src_target as (
    select
        src.source_system                                       as source_system,
        src.source_account_identifier                           as source_account_identifier,
        lkp_account.consolidation_account                       as target_account_identifier,
        lkp_account.source_subsidiary_id                        as source_subsidiary_id, 
        lkp_account.source_object_id                            as source_object_id,
        src.company_code                                        as source_company_code,
        src.company_code || '.' || src.source_business_unit_code
            || '.'|| src.source_object_id                                 
                                                                as source_concat_nat_key,
        {{
            dbt_utils.surrogate_key(
                ["src.source_system","source_concat_nat_key"]
             )
        }}                                                      as account_guid,
     --   ''                                                      as source_subledger_identifier,
     --   ''                                                      as source_subledger_type,
        src.company_code                                        as company_code,
        src.fiscal_period_number                                as fiscal_period_number,
        src.source_business_unit_code                           as source_business_unit_code,
        {{
            dbt_utils.surrogate_key(
                [
                 "src.source_system",
                 "src.source_business_unit_code",
                  "'PLANT_DC'",
                ]
            )
        }}                                                      as business_unit_address_guid,
        nvl(to_char(source_ledger_type_lkp.normalized_value), 
                to_char(src.source_ledger_type))                as ledger_type,
        'USD'                                                   as phi_currency,
        base.default_currency_code                              as base_currency,
        case when pcomp.parent_currency_code is null then
        'USD' else pcomp.parent_currency_code end               as pcomp_currency,
        case when upper(ltrim(rtrim(src.source_system)))='PHI'
        or upper(ltrim(rtrim(src.source_system))) = 'PCB'
        or upper( ltrim(rtrim(src.source_system))) = 'POST'
        or upper(ltrim(rtrim(src.source_system)))=
       '{{env_var("DBT_SOURCE_SYSTEM")}}' then 'Y' else 'N' end as incoming_equal_base_flag,
        nvl(to_char(transaction_currency_lkp.normalized_value), 
                to_char(src.transaction_currency))              as v_transaction_currency_1,
        case when v_transaction_currency_1 is null then '-' 
        else v_transaction_currency_1 end                       as v_transaction_currency_2, 
        case when incoming_equal_base_flag = 'Y'and 
        ltrim(rtrim(v_transaction_currency_2)) = '-' 
        then base_currency else 
        ltrim(rtrim(v_transaction_currency_2)) end              as txn_currency,
        src.prior_year_ending_bal                               as prior_year_ending_bal, 
        src.txn_period_change_amt                               as txn_period_change_amt, 
        src.txn_ytd_bal                                         as txn_ytd_bal, 
        xref.tagetik_account                                    as tagetik_account ,
        xref.node_1                                             as node_1,       
        src.source_updated_datetime                             as source_date_updated,
        dt.lkp_gl_date                                          as lkp_gl_date,
        trunc(dim_cal.calendar_date_id)                         as source_updated_d_id,
        current_timestamp()                                     as load_date,
        current_timestamp()                                     as update_date	

    from source src 
       left outer join lkp_account 
            on lkp_account.source_account_identifier = src.source_account_identifier            
            and lkp_account.source_company_code=src.company_code
            and lkp_account.source_business_unit_code=src.source_business_unit_code                 
       left outer join dim_date dt 
            on src.fiscal_period_number = dt.report_fiscal_year_period_no
       left outer join xref xref on xref.tagetik_account = lkp_account.consolidation_account
       left outer join dim_calender dim_cal 
            on dim_cal.calendar_date=src.source_updated_datetime
       left outer join base_curr base on src.source_system=base.source_system and 
            src.company_code=base.company_code and dt.lkp_gl_date>=base.effective_date and 
            dt.lkp_gl_date<=base.expiration_date
       left outer join pcomp_curr pcomp on src.source_system=pcomp.source_system and 
            src.company_code=pcomp.company_code and dt.lkp_gl_date>=pcomp.effective_date and 
            dt.lkp_gl_date<=pcomp.expiration_date
       left outer join
                {{ent_dbt_package.lkp_normalization("UPPER(ltrim(rtrim(src.source_system)))",
                "FINANCE","LEDGER_TYPE_CODE",
                "UPPER(ltrim(rtrim(src.source_ledger_type)))","source_ledger_type_lkp")}}
       left outer join
                {{ent_dbt_package.lkp_normalization("UPPER(ltrim(rtrim(src.source_system)))",
                "ADDRESS_BOOK","CUST_CURRENCY_CODE",
                "UPPER(ltrim(rtrim(src.transaction_currency)))","transaction_currency_lkp")}} 
),

curr_conv_rt as (
    select 
        cast(substring(tf.source_system,1,255) as text(255))              as source_system,
        cast(substring(tf.source_account_identifier,1,255) as text(255)) as source_account_identifier,
        tf.target_account_identifier                           as target_account_identifier,
        tf.source_subsidiary_id                                as source_subsidiary_id,
        tf.source_object_id                                    as source_object_id,
        tf.source_company_code                                 as source_company_code,
        tf.account_guid                                        as account_guid,
      --  cast(substring(tf.source_subledger_identifier,1,255) as text(255)) as source_subledger_identifier,
     --   cast(substring(tf.source_subledger_type,1,255) as text(255) )      as source_subledger_type,
        cast(substring(tf.company_code,1,20) as text(20))                     as company_code,
        cast(tf.fiscal_period_number as number(38,0))                         as fiscal_period_number,
        cast(substring(tf.source_business_unit_code,1,255) as text(255)) as source_business_unit_code,
        tf.business_unit_address_guid                          as business_unit_address_guid,
        cast(substring(tf.ledger_type,1,255) as text(255))     as ledger_type,
        cast(substring(tf.txn_currency,1,20) as text(20))      as txn_currency,
        tf.base_currency                                       as base_currency,
        tf.phi_currency                                        as phi_currency,
        tf.pcomp_currency                                      as pcomp_currency,
        case when tf.base_currency = tf.txn_currency then 1
        when upper(tf.node_1) in ('INCOMESTATEMENT',
        'OPERATINGEXPENSES','METRICS','SUPPLEMENTAL')
        then curr_exh_a.curr_conversion_rt
        when upper(tf.node_1) = 'BALANCESHEET'
        then curr_exh_m.curr_conversion_rt end                 as v_txn_conv_rt,
        case when v_txn_conv_rt is null then 0
        else v_txn_conv_rt end                                 as txn_conv_rt,
        case when v_txn_conv_rt = 0 then 0 else 1 
        end / v_txn_conv_rt                                    as base_conv_rt,
        case when tf.base_currency = tf.phi_currency then 1
        when upper(tf.node_1) in ('INCOMESTATEMENT',
        'OPERATINGEXPENSES','METRICS','SUPPLEMENTAL')
        then curr_exh_a.curr_conversion_rt
        when upper(tf.node_1) = 'BALANCESHEET'
        then curr_exh_m.curr_conversion_rt end                 as v_phi_conv_rt,
        case when v_phi_conv_rt is null then 0
        else v_phi_conv_rt end                                 as phi_conv_rt,
        case when tf.base_currency = tf.pcomp_currency then 1
        when upper(tf.node_1) in ('INCOMESTATEMENT',
        'OPERATINGEXPENSES','METRICS','SUPPLEMENTAL')
        then curr_exh_a.curr_conversion_rt
        when upper(tf.node_1) = 'BALANCESHEET'
        then curr_exh_m.curr_conversion_rt end                 as v_pcomp_conv_rt,
        case when v_pcomp_conv_rt is null then 0
        else v_pcomp_conv_rt end                               as pcomp_conv_rt,
        case when regexp_instr(tf.tagetik_account,'2')=1  
        or regexp_instr(tf.tagetik_account,'4')=1 then
        tf.prior_year_ending_bal*-1 else 
        tf.prior_year_ending_bal end                           as txn_prior_year_ending_bal,
        txn_prior_year_ending_bal * base_conv_rt               as base_prior_year_ending_bal,
        base_prior_year_ending_bal * v_phi_conv_rt             as phi_prior_year_ending_bal,
        base_prior_year_ending_bal * v_pcomp_conv_rt           as pcomp_prior_year_ending_bal,
        case when regexp_instr(tf.tagetik_account,'2')=1  
        or regexp_instr(tf.tagetik_account,'4')=1 then 
        tf.txn_period_change_amt*-1 else 
        tf.txn_period_change_amt end                           as txn_period_change_amt,
        txn_period_change_amt * base_conv_rt                   as base_period_change_amt,
        base_period_change_amt * v_phi_conv_rt                 as phi_period_change_amt,
        base_period_change_amt * v_pcomp_conv_rt               as pcomp_period_change_amt,
        case when regexp_instr(tf.tagetik_account,'2')=1  
        or regexp_instr(tf.tagetik_account,'4')=1 then 
        tf.txn_ytd_bal*-1 else tf.txn_ytd_bal end              as txn_ytd_bal,
        txn_ytd_bal                                            as base_ytd_bal,
        base_ytd_bal * v_phi_conv_rt                           as phi_ytd_bal,
        base_ytd_bal * v_pcomp_conv_rt                         as pcomp_ytd_bal,
        tf.source_date_updated                                 as source_date_updated,
        tf.lkp_gl_date                                         as lkp_gl_date,
        current_timestamp()                                    as load_date,
        current_timestamp()                                    as update_date,
        tf.source_updated_d_id                                 as source_updated_d_id
       
    from src_target tf
      --  left outer join xref xref on xref.tagetik_account = lkp_account.consolidation_account
        left outer join
            {{
                ent_dbt_package.lkp_exchange_rate_month(
                    "base_currency", "txn_currency", "fiscal_period_number", "'A'","curr_exh_a")
                
            }} 
        left outer join
            {{
                ent_dbt_package.lkp_exchange_rate_month(
                    "base_currency", "txn_currency", "fiscal_period_number", "'M'","curr_exh_m")
            }}
   
),
final as (
      select
          {{
            dbt_utils.surrogate_key(
                [
                    "rt.source_system",
                    "rt.source_account_identifier",
                    "rt.source_business_unit_code",
                    "rt.company_code",
                    "rt.fiscal_period_number",
                    "rt.txn_currency",
                    "rt.ledger_type",
                ]
            ) 
        }}                                  as unique_key,
        rt.source_system                    as source_system,
        rt.source_account_identifier        as source_account_identifier,
        rt.target_account_identifier        as target_account_identifier,
        rt.source_subsidiary_id             as source_subsidiary_id,
        rt.source_object_id                 as source_object_id,
        rt.source_company_code              as source_company_code,
        rt.account_guid                     as account_guid,
       -- rt.source_subledger_identifier      as source_subledger_identifier,
       -- rt.source_subledger_type            as source_subledger_type,
        rt.company_code                     as company_code,
        rt.fiscal_period_number             as fiscal_period_number,
        rt.source_business_unit_code        as source_business_unit_code,
        rt.business_unit_address_guid       as business_unit_address_guid,
        rt.ledger_type                      as ledger_type,
        rt.txn_currency                     as txn_currency,
        rt.base_currency                    as base_currency,
        rt.phi_currency                     as phi_currency,
        rt.pcomp_currency                   as pcomp_currency,
        rt.txn_conv_rt                      as txn_conv_rt,
        rt.base_conv_rt                     as base_conv_rt,
        rt.phi_conv_rt                      as phi_conv_rt,
        rt.pcomp_conv_rt                    as pcomp_conv_rt,
        rt.txn_prior_year_ending_bal        as txn_prior_year_ending_bal,
        rt.txn_period_change_amt            as txn_period_change_amt,
        rt.txn_ytd_bal                      as txn_ytd_bal,
        rt.base_prior_year_ending_bal       as base_prior_year_ending_bal,
        rt.base_period_change_amt           as base_period_change_amt,
        rt.base_ytd_bal                     as base_ytd_bal,
        case when rt.phi_prior_year_ending_bal is null 
        then 0 else rt.phi_prior_year_ending_bal end    as phi_prior_year_ending_bal,
        case when rt.phi_period_change_amt is null
        then 0 else rt.phi_period_change_amt end        as phi_period_change_amt,
        case when rt.phi_ytd_bal is null
        then 0 else rt.phi_ytd_bal end                  as phi_ytd_bal,
        case when rt.pcomp_prior_year_ending_bal is null
        then 0 else rt.pcomp_prior_year_ending_bal end  as pcomp_prior_year_ending_bal,
        case when rt.pcomp_period_change_amt is null
        then 0 else rt.pcomp_period_change_amt end      as pcomp_period_change_amt,
        case when rt.pcomp_ytd_bal is null
        then 0 else rt.pcomp_ytd_bal end                as pcomp_ytd_bal,
        rt.source_date_updated              as source_date_updated,
        rt.source_updated_d_id              as source_updated_d_id,
        current_timestamp()                 as load_date,
        current_timestamp()                 as update_date             
          
    from curr_conv_rt rt
)

select * from final