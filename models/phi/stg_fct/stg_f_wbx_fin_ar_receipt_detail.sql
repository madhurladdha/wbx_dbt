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

    custinvoicejour as (select * from {{ ref("src_custinvoicejour") }}),

    custsettlement as (select * from {{ ref("src_custsettlement") }}),

    gje as (select * from {{ ref("src_generaljournalentry") }}),

    gjae as (select * from {{ ref("src_generaljournalaccountentry") }}),

    mainaccount as (select * from {{ ref("src_mainaccount") }}),

    salestable as (select * from {{ ref("src_salestable") }}),

    custtransfilter as (
        select 
            distinct upper(trim(dataareaid)) as company, 
            trim(voucher) as payvoucher
        from custtrans
        where transtype in (9, 15, 27)
    ),

    journal as (
        select
            gje.subledgervoucher,
            gje.subledgervoucherdataareaid,
            max(gje.journalnumber) as journalnumber,
            max(gje.documentdate) as documentdate,
            min(gjae.mainaccount) as mainaccount
        from gje, gjae
        where gje.recid = gjae.generaljournalentry and gjae.postingtype in (31, 20)
        group by gje.subledgervoucher, gje.subledgervoucherdataareaid
    ),

    journaldiscount as (
        (
            select
                gje.subledgervoucher,
                gje.accountingdate,
                gje.subledgervoucherdataareaid,
                gje.journalnumber,
                gje.documentdate,
                gje.ledgerentryjournal,
                max(gjae.mainaccount) as mainaccount
            from gje, gjae
            where gje.recid = gjae.generaljournalentry and gjae.postingtype in (34, 53)
            group by
                gje.subledgervoucher,
                gje.accountingdate,
                gje.subledgervoucherdataareaid,
                gje.journalnumber,
                gje.documentdate,
                gje.ledgerentryjournal
        )
    )



select
    '{{ env_var("DBT_SOURCE_SYSTEM") }}' as source_system,
    cast(ctrp.payvoucher as varchar(255)) as payment_identifier,
    row_number() over (partition by ctrp.payvoucher order by ctri.recid) as line_number,
    cast(ctri.paymreference as varchar(255)) as receipt_number,
    cast(ctri.voucher as varchar(255)) as document_number,
    cast('C_' || substr(ctri.voucher, 1, 3) as varchar(255)) as document_type,
    cast(upper(trim(ctri.dataareaid)) as varchar(255)) as document_company,
    cast(ctri.invoice as varchar(255)) as document_pay_item,
    cstl.transdate as gl_date,
    cast(
        case when gj.mainaccount is null then 'N' else 'P' end as varchar(6)
    ) as gl_posted_flag,
    case
        when (trim(gj.mainaccount)) IS null or (trim(gj.mainaccount)) = ''
        then '-'
        else cast(gj.mainaccount as varchar(255))
    end as source_account_identifier,
    cast(upper(trim(ctrp.company)) as varchar(255)) as company_code,
    cast(ctri.transtype as varchar(255)) as batch_type,
    cast(gj.journalnumber as varchar(255)) as batch_number,
    gj.documentdate as batch_date,
    cast(
        case when trim(cij.taxgroup) = 'DOM' then 'D' else 'F' end as varchar(255)
    ) as foreign_transaction_flag,
    case
        when (trim(gjd.mainaccount)) IS null or (trim(gjd.mainaccount)) = ''
        then '-'
        else cast(gjd.mainaccount as varchar(255))
    end as discount_src_acct_id,
    cast(null as varchar(255)) as writeoff_reason_code,
    cast('-' as varchar(255)) as writeoff_src_acct_id,
    cast(null as varchar(255)) as chargeback_reason_code,
    cast('-' as varchar(255)) as chargeback_src_acct_id,
    cast(null as varchar(255)) as deduction_reason_code,
    cast('-' as varchar(255)) as deduction_src_acct_id,
    cast('-' as varchar(255)) as source_business_unit_code,
    cast(ctri.txt as varchar(255)) as remark_txt,
    cast('-' as varchar(255)) as deduction_document_number,
    cast('-' as varchar(255)) as deduction_document_type,
    cast('-' as varchar(255)) as deduction_document_company,
    cast('-' as varchar(255)) as deduction_document_pay_item,
    cast('-' as varchar(255)) as journal_document_type,
    cast(gj.journalnumber as varchar(255)) as journal_document_number,
    cast(upper(trim(ctrp.company)) as varchar(255)) as journal_document_company,
    cast(null as varchar(255)) as void_date,
    cast(null as varchar(255)) as void_reason_code,
    cast(null as varchar(255)) as receipt_type_code,
    ctri.duedate as net_due_date,
    cij.cashdiscdate as discount_date,
    ctri.transdate as inv_jrnl_date,
    cast(null as varchar(255)) as reference_txt,
    cast(ctri.accountnum as varchar(255)) as source_customer_identifier,
    cast('-' as varchar(255)) as source_payor_identifier,
    cast(ctri.paymmode as varchar(255)) as source_payment_instr_code,
    cast(ctri.currencycode as varchar(255)) as txn_currency,
    cstl.settleamountcur as txn_payment_amt,
    cij.sumlinedisc as txn_discount_avail_amt,
    cij.sumlinedisc as txn_discount_taken_amt,
    0 as txn_writeoff_amt,
    0 as txn_chargeback_amt,
    0 as txn_deduction_amt,
    cstl.exchadjustment as txn_gain_loss_amt,
    cstl.settleamountmst as base_payment_amt,
    cij.sumlinediscmst as base_discount_avail_amt,
    cij.sumlinediscmst as base_discount_taken_amt,
    0 as base_writeoff_amt,
    0 as base_chargeback_amt,
    0 as base_deduction_amt,
    cast(
        case
            when cast(ctri.amountcur as decimal(38, 10)) <> 0
            then
                cast(
                    (cstl.exchadjustment::float * ctri.amountmst::float) as decimal(38, 10)
                )
                / cast(ctri.amountcur::float as decimal(38, 10))
            else 0
        end as decimal(38, 10)
    ) as base_gain_loss_amt,  -- not currently used
    cij.sumtax as txn_tax_amt,
    cast('N' as varchar(1)) as intercompany_flag,
    ma.mainaccountid as source_object_code,
    ctri.modifieddatetime as source_updated_datetime
from
    custtransfilter ctrp
inner join
    custsettlement cstl
    on trim(ctrp.payvoucher) = trim(cstl.offsettransvoucher)
    and ctrp.company = upper(trim(cstl.dataareaid))
inner join custtrans ctri on ctri.recid = cstl.transrecid
left outer join
    custinvoicejour cij
    on ctri.invoice = cij.invoiceid
    and ctri.voucher = cij.ledgervoucher
    and upper(trim(ctri.dataareaid)) = upper(trim(cij.dataareaid))
/* THIS JOIN RETRIEVES THE AR ACCOUNT */
left outer join 
    journal gj
    on trim(ctri.voucher) = trim(gj.subledgervoucher)
    and trim(upper(ctri.dataareaid)) = trim(upper(gj.subledgervoucherdataareaid))
left join mainaccount ma 
    on ma.recid = gj.mainaccount
/* THIS JOIN RETRIEVES THE DISCOUNT ACCOUNT */
left outer join 
    journaldiscount gjd
    on trim(ctri.voucher) = trim(gjd.subledgervoucher)
    and trim(upper(ctri.dataareaid)) = trim(upper(gjd.subledgervoucherdataareaid))
    and ctri.transdate = gjd.accountingdate
left join 
    mainaccount mad on mad.recid = gjd.mainaccount
left join
    salestable st
    on cij.salesid = st.salesid
    and trim(upper(cij.dataareaid)) = trim(upper(st.dataareaid))
