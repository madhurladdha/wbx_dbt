{{ config(tags=["finance", "gl","gl_monthly"]) }}

/* transactional table there is no date column, todo the FULL REFRESH  */
{% set now = modules.datetime.datetime.now() %}
{%- set full_load_day -%} {{env_var('DBT_FULL_LOAD_DAY')}} {%- endset -%}
{%- set day_today -%} {{ now.strftime('%A') }} {%- endset -%}

with dimensionfocusbalance as (
    select * from {{ ref('src_dimensionfocusbalance')}}
),

ledger as (
    select * from {{ ref('src_ledger')}}
),

dimensionattributevaluecombo as (
    select * from {{ ref('src_dimensionattributevaluecombo')}}
),

dimensionhierarchy as (
    select * from {{ ref('src_dimensionhierarchy')}}
),

fiscalcalendarperiod as (
    select * from {{ ref('src_fiscalcalendarperiod')}}
),

fiscalcalendaryear as (
    select * from {{ ref('src_fiscalcalendaryear')}}
),

mainaccount as (
    select * from {{ ref('src_mainaccount')}}
),

accts as (
    select 
            distinct ma.recid as source_account_identifier, 
            ma.type, substr(davc.displayvalue, 1,
            regexp_instr(davc.displayvalue, '~') - 1) as source_object_id,
            dfb.focusledgerdimension,
            case when trim(substr(davc.displayvalue, 
            regexp_instr(davc.displayvalue, '~') + 1)) = '' then '-' 
            else substr(davc.displayvalue, 
            regexp_instr(davc.displayvalue, '~') + 1) end as source_business_unit_code, 
            l.name as company_code, 
            case when trim(l.accountingcurrency) = '' then '-' else 
            trim(l.accountingcurrency) end as transaction_currency, 
            fcp.month as fiscal_month, 
            cast(fcy.name as int) as fiscal_calendar_year, 
            cast(fcy.name as int)*100+fcp.month+1 as fiscal_period_number
    from dimensionfocusbalance dfb
        inner join ledger l on dfb.ledger = l.recid
        inner join dimensionattributevaluecombo davc 
            on dfb.focusledgerdimension = davc.recid
        inner join dimensionhierarchy dh 
            on dfb.focusdimensionhierarchy = dh.recid
        inner join fiscalcalendarperiod fcp 
            on l.fiscalcalendar = fcp.fiscalcalendar
        --and dfb.accountingdate >= fcp.startdate and dfb.accountingdate <= fcp.enddate
        inner join fiscalcalendaryear fcy 
            on fcp.fiscalcalendaryear = fcy.recid
        inner join mainaccount ma 
            on ma.mainaccountid = trim(substr(davc.displayvalue, 1, regexp_instr(davc.displayvalue, '~') - 1)) 
        and trim(upper(dh.name)) = 'MA+CC' and ma.source = davc.source

),
ob as (
    select 
        ma.recid as source_account_identifier, 
        substr(davc.displayvalue,1,regexp_instr(davc.displayvalue,'~')-1) as source_object_id,
        case when trim(substr(davc.displayvalue,regexp_instr(davc.displayvalue, '~') + 1)) 
        = '' then '-' else trim(substr(davc.displayvalue, 
        regexp_instr(davc.displayvalue, '~') + 1)) end as source_business_unit_code, 
        l.name as company_code,
        cast(fcy.name as int) as fiscal_calendar_year, 
        dfb.accountingdate, 
        fcp.fiscalcalendaryear,
        sum(coalesce(dfb.creditaccountingcurrencyamount,0) 
        + coalesce(dfb.debitaccountingcurrencyamount,0))* -1 as opening_bal
    from dimensionfocusbalance dfb
        inner join ledger l on dfb.ledger = l.recid
        inner join dimensionattributevaluecombo davc 
            on dfb.focusledgerdimension = davc.recid
        inner join dimensionhierarchy dh 
            on dfb.focusdimensionhierarchy = dh.recid
        inner join fiscalcalendarperiod fcp 
            on dfb.fiscalcalendarperiodtype = fcp.type and l.fiscalcalendar = fcp.fiscalcalendar
        inner join fiscalcalendaryear fcy 
            on fcp.fiscalcalendaryear = fcy.recid
        inner join mainaccount ma 
            on ma.mainaccountid = trim(substr(davc.displayvalue, 1,
            regexp_instr(davc.displayvalue, '~') - 1))  and ma.source = davc.source
    where 
        dfb.fiscalcalendarperiodtype = 2 and dfb.accountingdate = fcy.enddate
        and trim(upper(dh.name)) = 'MA+CC'
        group by ma.recid, substr(davc.displayvalue, 1, regexp_instr(davc.displayvalue, '~') - 1),
        case when trim(substr(davc.displayvalue, regexp_instr(davc.displayvalue, '~') + 1)) 
        = '' then '-' else trim(substr(davc.displayvalue, regexp_instr(davc.displayvalue, '~') 
        + 1)) end, l.name, fcy.name, fcp.fiscalcalendaryear, dfb.accountingdate
),
pd as (
     select 
        ma.recid as source_account_identifier, 
        substr(davc.displayvalue,1,regexp_instr(davc.displayvalue,'~')-1) as source_object_id,
        case when trim(substr(davc.displayvalue, regexp_instr(davc.displayvalue, '~') + 1)) = '' 
        then '-' else substr(davc.displayvalue, regexp_instr(davc.displayvalue, '~') + 1) end as source_business_unit_code,
        l.name as company_code, 
        fcp.month as fiscal_month,  
        fcp.name as fiscal_period_number,
        cast(fcy.name as int) as fiscal_calendar_year,
        case when trim(to_char(davc.ledgerdimensiontype)) = '' then '-' else 
        to_char(davc.ledgerdimensiontype) end  as ledgerdimensiontype,
        sum(coalesce(dfb.creditaccountingcurrencyamount,0) 
        + coalesce(dfb.debitaccountingcurrencyamount,0)) as txn_ledger_bal
    from dimensionfocusbalance dfb
        inner join ledger l on dfb.ledger = l.recid
        inner join dimensionattributevaluecombo davc 
            on dfb.focusledgerdimension = davc.recid
        inner join dimensionhierarchy dh 
            on dfb.focusdimensionhierarchy = dh.recid
        inner join fiscalcalendarperiod fcp
            on dfb.fiscalcalendarperiodtype = fcp.type and l.fiscalcalendar = fcp.fiscalcalendar
        inner join fiscalcalendaryear fcy 
            on fcp.fiscalcalendaryear = fcy.recid
        inner join mainaccount ma 
            on ma.mainaccountid = trim(substr(davc.displayvalue, 1, regexp_instr(davc.displayvalue, '~') - 1))   and ma.source = davc.source
        where dfb.fiscalcalendarperiodtype = 1 
        and dfb.accountingdate >= fcp.startdate and dfb.accountingdate <= fcp.enddate
        and trim(upper(dh.name)) = 'MA+CC'
    group by 
        ma.recid, substr(davc.displayvalue, 1, regexp_instr(davc.displayvalue, '~') - 1),
        case when trim(substr(davc.displayvalue, regexp_instr(davc.displayvalue, '~') + 1)) = '' then  '-' 
        else substr(davc.displayvalue, regexp_instr(davc.displayvalue, '~') + 1) end,
        l.name, fcy.name, fcp.month, fcp.name, fcp.fiscalcalendaryear, davc.ledgerdimensiontype
),
source as (
    SELECT 
        '{{env_var("DBT_SOURCE_SYSTEM")}}' as source_system,
        accts.source_account_identifier, 
        accts.company_code, 
        accts.source_object_id, 
        accts.fiscal_period_number,
        ob.opening_bal as prior_year_ending_bal,
        pd.txn_ledger_bal as txn_period_change_amt,
        case when accts.type = 3 then coalesce(ob.opening_bal,0) + sum(coalesce(pd.txn_ledger_bal,0)) 
        over (partition by accts.fiscal_calendar_year, accts.company_code, 
        accts.source_object_id, accts.source_business_unit_code order by 
        accts.fiscal_period_number) else sum(coalesce(pd.txn_ledger_bal,0)) 
        over (partition by accts.fiscal_calendar_year, accts.company_code, 
        accts.source_object_id, accts.source_business_unit_code 
        order by accts.fiscal_period_number) end txn_ytd_bal,
        accts.source_business_unit_code,
        accts.transaction_currency,
        coalesce(pd.ledgerdimensiontype, '-') as source_ledger_type, 
        null as source_updated_datetime
    from accts left join ob
        on accts.source_account_identifier = ob.source_account_identifier
        and accts.source_object_id = ob.source_object_id
        and accts.source_business_unit_code = ob.source_business_unit_code 
        and ob.fiscal_calendar_year = accts.fiscal_calendar_year - 1
        and accts.company_code = ob.company_code
    left join pd
        on accts.source_account_identifier = pd.source_account_identifier 
        and accts.company_code = pd.company_code 
        and pd.source_object_id = accts.source_object_id
        and pd.source_business_unit_code = accts.source_business_unit_code 
        and accts.fiscal_calendar_year = pd.fiscal_calendar_year 
        and accts.fiscal_month = pd.fiscal_month
        where abs(ob.opening_bal) > 0 or ob.opening_bal is null
),
   final as (
    select
       '{{env_var("DBT_SOURCE_SYSTEM")}}'                              as source_system,
        cast(substring(source_account_identifier,1,255) as text(255))  as source_account_identifier, 
        src.company_code                                               as company_code,
        src.source_object_id                                           as source_object_id,
        src.fiscal_period_number                                       as fiscal_period_number,
        src.prior_year_ending_bal                                      as prior_year_ending_bal, 
        src.txn_period_change_amt                                      as txn_period_change_amt, 
        src.txn_ytd_bal                                                as txn_ytd_bal, 
        case when src.source_business_unit_code is null then '-' 
        else source_business_unit_code end                             as source_business_unit_code, 
        src.transaction_currency                                       as transaction_currency, 
        src.source_ledger_type                                         as source_ledger_type, 
        to_date(substr(src.source_updated_datetime,1,10),'MM/DD/YYYY') as source_updated_datetime
    from source src
    where (abs(src.txn_ytd_bal) > 0 or abs(src.prior_year_ending_bal) > 0)
)
 select * from final