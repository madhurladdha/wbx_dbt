
{{ config(tags=["finance", "ar"]) }}

{% set now = modules.datetime.datetime.now() %}
{%- set full_load_day -%} {{env_var('DBT_FULL_LOAD_DAY')}} {%- endset -%}
{%- set day_today -%} {{ now.strftime('%A') }} {%- endset -%}

with
    custtrans as (
        select *
        from {{ ref("src_custtrans") }}

        /*  The filter is conditionally applied for different scenarios that can occur.
        We want to do a full refresh of the downstream FACT if 2 conditions exist:
            1) It is the designated full refresh day.  Typically this is Sunday for PHI.  Parameter = DBT_FULL_LOAD_DAY
            2) The command line for the job includes the flag of '--full-refresh'
        So the filter is applied in all other cases, which is when the incremental load is required.  Typically Mon - Sat.
        This ensures that only the past X number of day's data are processed.  Parameter = DBT_STD_INCR_LOOKBACK.
        Basically, if it is NOT full load day AND NOT a command line full-refresh, then we apply the filter and process incrementally.
    */
        {% if day_today != full_load_day %}
        {% if not flags.FULL_REFRESH %}
        where transdate >= current_date() - {{ env_var("DBT_STD_INCR_LOOKBACK") }}
        {% endif %}
        {% endif %}
    ),

    custtable as (select * from {{ ref("src_custtable") }}),

    custinvoicejour as (select * from {{ ref("src_custinvoicejour") }}),

    custsettlement as (select * from {{ ref("src_custsettlement") }}),

    gje as (select * from {{ ref("src_generaljournalentry") }}),

    gjae as (select * from {{ ref("src_generaljournalaccountentry") }}),

    mainaccount as (select * from {{ ref("src_mainaccount") }}),

    salestable as (select * from {{ ref("src_salestable") }}),

    journal as (
        select
            gje.subledgervoucher,
            gje.subledgervoucherdataareaid,
            max(gje.journalnumber) as journalnumber,
            max(gje.documentdate) as documentdate,
            min(gjae.mainaccount) as mainaccount
        from gje, gjae
        where
            gje.recid = gjae.generaljournalentry
            and gjae.postingtype in (31, 53, 60, 40)
        group by gje.subledgervoucher, gje.subledgervoucherdataareaid
    ),

    cstl as (
        select
            transrecid,
            sum(settleamountmst) - sum(exchadjustment) as settlemsttot,
            sum(exchadjustment) as gainloss,
            sum(settleamountcur) as settlecurtot
        from custsettlement
        group by transrecid
    )

select
    '{{ env_var("DBT_SOURCE_SYSTEM") }}' as source_system,
    cast(custtrans.voucher as varchar(255)) as document_number,
    cast('C_' || substr(custtrans.voucher, 1, 3) as varchar(255)) as document_type,
    custtrans.dataareaid as document_company,
    custtrans.accountnum as source_customer_identifier,
    custtrans.transdate as gl_date,
    custtrans.transdate as invoice_date,
    cast(custtrans.transtype as varchar(255)) as batch_type,
    cast(journal.journalnumber as varchar(255)) as batch_number,
    journal.documentdate as batch_date,
    custtrans.dataareaid as company_code,
    cast(journal.mainaccount as varchar(255)) as source_account_identifier,
    cast(
        case when journal.mainaccount is null then 'N' else 'P' end as varchar(6)
    ) as gl_posted_flag,
    cast(
        case
            when
                custtrans.closed <> '01-JAN-1900'
                and custtrans.amountcur = custtrans.settleamountcur
            then 'PAID_IN_FULL'
            else 'OPEN'
        end as varchar(255)
    ) as pay_status_code,
    cast(
        case when custinvoicejour.taxgroup = 'DOM' then 'D' else 'F' end as varchar(255)
    ) as foreign_transaction_flag,
    custtrans.transdate as service_date,
    cast('-' as varchar(255)) as source_business_unit_code,
    cast(
        case
            when trim(custinvoicejour.payment) = ''
            then trim(custtable.paymtermid)
            when trim(custinvoicejour.payment) is null
            then trim(custtable.paymtermid)
            else trim(custinvoicejour.payment)
        end as varchar(255)
    ) as source_payment_terms_code,
    custtrans.duedate as net_due_date,
    cast(custtrans.invoice as varchar(255)) as sales_document_number,
    cast(
        case
            when trim(custtrans.invoice) <> ''
            then 'C_' || substr(custtrans.invoice, 1, 3)
            else null
        end as varchar(255)
    ) as sales_document_type,
    custtrans.dataareaid as sales_document_company,
    custtrans.closed as cleared_date,
    cast(custtrans.txt as varchar(255)) as reference_txt,
    cast(custinvoicejour.deliveryname as varchar(255)) as name_txt,
    cast(custtrans.paymmode as varchar(255)) as source_payment_instr_code,
    cast(custtrans.lastsettlevoucher as varchar(255)) as payment_identifier,
    custtrans.closed as invoice_closed_date,
    cast(custtrans.currencycode as varchar(255)) as txn_currency,
    custtrans.amountcur as txn_gross_amt,
    custtrans.amountcur - coalesce(cstl.settlecurtot, 0) as txn_open_amt,
    coalesce(custinvoicejour.sumlinedisc, 0) as txn_discount_available_amt,
    coalesce(custinvoicejour.sumlinedisc, 0) as txn_discount_taken_amt,
    case
        when coalesce(custinvoicejour.sumtax, 0) <> 0 then custtrans.amountcur else 0
    end as txn_taxable_amt,
    case
        when coalesce(custinvoicejour.sumtax, 0) = 0 then custtrans.amountcur else 0
    end as txn_nontaxable_amt,
    coalesce(custinvoicejour.sumtax, 0) as txn_tax_amt,
    coalesce(custinvoicejour.summarkup, 0) as txn_purchase_charge_amt,
    custtrans.amountmst as base_gross_amt,
    custtrans.amountmst - coalesce(cstl.settlemsttot, 0) as base_open_amt,
    coalesce(custinvoicejour.sumlinediscmst, 0) as base_discount_available_amt,
    coalesce(custinvoicejour.sumlinediscmst, 0) as base_discount_taken_amt,
    case
        when coalesce(custinvoicejour.sumtaxmst, 0) <> 0 then custtrans.amountmst else 0
    end as base_taxable_amt,
    case
        when coalesce(custinvoicejour.sumtaxmst, 0) = 0 then custtrans.amountmst else 0
    end as base_nontaxable_amt,
    coalesce(custinvoicejour.sumtaxmst, 0) as base_tax_amt,
    coalesce(custinvoicejour.summarkupmst, 0) as base_purchase_charge_amt,
    cast('N' as varchar(1)) as intercompany_flag,
    mainaccount.mainaccountid as source_object_code,
    custtrans.modifieddatetime as source_updated_datetime
from custtrans
inner join
    custtable
    on custtable.accountnum = custtrans.accountnum
    and custtrans.dataareaid = custtable.dataareaid
left outer join
    custinvoicejour
    on trim(custtrans.invoice) = trim(custinvoicejour.invoiceid)
    and custtrans.voucher = custinvoicejour.ledgervoucher
    and custtrans.dataareaid = custinvoicejour.dataareaid
left outer join cstl on custtrans.recid = cstl.transrecid
left outer join
    journal
    on trim(custtrans.voucher) = trim(journal.subledgervoucher)
    and custtrans.dataareaid = journal.subledgervoucherdataareaid
left join mainaccount on mainaccount.recid = journal.mainaccount
left join
    salestable
    on custinvoicejour.salesid = salestable.salesid
    and custinvoicejour.dataareaid = salestable.dataareaid
where custtrans.transtype not in (9, 15, 27)

union

select
    '{{ env_var("DBT_SOURCE_SYSTEM") }}' as source_system,
    cast(
        custtrans.voucher || '.' || to_char(custtrans.recid) as varchar(255)
    ) as document_number,
    cast('C_' || substr(custtrans.voucher, 1, 3) as varchar(255)) as document_type,
    custtrans.dataareaid as document_company,
    custtrans.accountnum as source_customer_identifier,
    custtrans.transdate as gl_date,
    custtrans.transdate as invoice_date,
    cast(custtrans.transtype as varchar(255)) as batch_type,
    cast(journal.journalnumber as varchar(255)) as batch_number,
    journal.documentdate as batch_date,
    custtrans.dataareaid as company_code,
    cast(journal.mainaccount as varchar(255)) as source_account_identifier,
    cast(
        case when journal.mainaccount is null then 'N' else 'P' end as varchar(6)
    ) as gl_posted_flag,
    cast(
        case
            when
                custtrans.closed <> '01-JAN-1900'
                and custtrans.amountcur = custtrans.settleamountcur
            then 'PAID_IN_FULL'
            else 'OPEN'
        end as varchar(255)
    ) as pay_status_code,
    cast(null as varchar(255)) as foreign_transaction_flag,
    custtrans.transdate as service_date,
    cast('-' as varchar(255)) as source_business_unit_code,
    cast(trim(custtable.paymtermid) as varchar(255)) as source_payment_terms_code,
    custtrans.duedate as net_due_date,
    cast(custtrans.invoice as varchar(255)) as sales_document_number,
    cast(
        case
            when trim(custtrans.invoice) <> ''
            then 'C_' || substr(custtrans.invoice, 1, 3)
            else null
        end as varchar(255)
    ) as sales_document_type,
    custtrans.dataareaid as sales_document_company,
    custtrans.closed as cleared_date,
    cast(custtrans.txt as varchar(255)) as reference_txt,
    cast(null as varchar(255)) as name_txt,
    cast(custtrans.paymmode as varchar(255)) as source_payment_instr_code,
    cast(custtrans.lastsettlevoucher as varchar(255)) as payment_identifier,
    custtrans.closed as invoice_closed_date,
    cast(custtrans.currencycode as varchar(255)) as txn_currency,
    custtrans.amountcur as txn_gross_amt,
    custtrans.amountcur as txn_open_amt,
    0 as txn_discount_available_amt,
    0 as txn_discount_taken_amt,
    0 as txn_taxable_amt,
    custtrans.amountcur as txn_nontaxable_amt,
    0 as txn_tax_amt,
    0 as txn_purchase_charge_amt,
    custtrans.amountmst as base_gross_amt,
    custtrans.amountmst as base_open_amt,
    0 as base_discount_available_amt,
    0 as base_discount_taken_amt,
    0 as base_taxable_amt,
    custtrans.amountmst as base_nontaxable_amt,
    0 as base_tax_amt,
    0 as base_purchase_charge_amt,
    cast('N' as varchar(1)) as intercompany_flag,
    mainaccount.mainaccountid as source_object_code,
    custtrans.modifieddatetime as source_updated_datetime
from custtrans
inner join
    custtable
    on custtable.accountnum = custtrans.accountnum
    and custtrans.dataareaid = custtable.dataareaid
left outer join
    journal
    on trim(custtrans.voucher) = trim(journal.subledgervoucher)
    and custtrans.dataareaid = journal.subledgervoucherdataareaid
left join mainaccount on mainaccount.recid = journal.mainaccount
where
    custtrans.transtype in (9, 15, 27)
    and custtrans.recid not in (select transrecid from custsettlement)
