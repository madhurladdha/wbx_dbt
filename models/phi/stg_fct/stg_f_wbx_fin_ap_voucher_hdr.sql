{{ config(tags=["finance", "ap"]) }}

{% set now = modules.datetime.datetime.now() %}
{%- set full_load_day -%} {{env_var('DBT_FULL_LOAD_DAY')}} {%- endset -%}
{%- set day_today -%} {{ now.strftime('%A') }} {%- endset -%}

with
    vendtrans as (
        select *
        from {{ ref("src_vendtrans") }}

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

    vendtable as (select * from {{ ref("src_vendtable") }}),

    vendinvoicejour as (
        select
            upper(trim(dataareaid)) as dataareaid,
            ledgervoucher,
            invoiceid,
            payment,
            max(salesbalance) as salesbalance,
            max(invoiceamount) as invoiceamount,
            max(sumtax) as sumtax,
            max(invoiceroundoff) as invoiceroundoff
        from {{ ref("src_vendinvoicejour") }}
        group by upper(trim(dataareaid)), ledgervoucher, invoiceid, payment
    ),

    vendsettlement as (select * from {{ ref("src_vendsettlement") }}),

    generaljournalentry as (select * from {{ ref("src_generaljournalentry") }}),

    generaljournalaccountentry as (
        select * from {{ ref("src_generaljournalaccountentry") }}
    ),

    mainaccount as (select * from {{ ref("src_mainaccount") }}),

    journal as (
        select
            gje.subledgervoucher,
            gje.accountingdate,
            gje.subledgervoucherdataareaid,
            min(gjae.mainaccount) as mainaccount,
            max(gje.journalnumber) as journalnumber
        from generaljournalentry gje, generaljournalaccountentry gjae
        where gje.recid = gjae.generaljournalentry and gjae.postingtype = 41
        group by
            gje.subledgervoucher, gje.accountingdate, gje.subledgervoucherdataareaid
    ),

    src_qualifier1 as (
        select
            '{{ env_var("DBT_SOURCE_SYSTEM") }}' as source_system,
            cast(upper(trim(vnd.dataareaid)) as varchar(255)) as document_company,
            cast(vnd.voucher as varchar(255)) as document_number,
            cast('V_' || substr(vnd.voucher, 1, 3) as varchar(255)) as document_type,
            cast('-' as varchar(255)) as source_business_unit_code,
            cast(vnd.accountnum as varchar(255)) as source_supplier_identifier,
            cast(
                iff(
                    trim(vnd.paymtermid) = '' or trim(vnd.paymtermid) is null,
                    iff(
                        trim(vij.payment) = '' or trim(vij.payment) is null,
                        'IMM',
                        trim(vij.payment)
                    ),
                    trim(vnd.paymtermid)
                ) as varchar(255)
            ) as source_payment_terms_code,
            cast(gj.mainaccount as varchar(255)) as source_account_identifier,
            cast(null as varchar(255)) as source_payee_identifier,
            cast(
                upper(trim(vnd.paymmode)) as varchar(255)
            ) as source_payment_instr_code,
            vnd.transdate as invoice_date,
            vnd.documentdate as voucher_date,
            vnd.duedate as net_due_date,
            cast(null as date) as discount_date,
            gj.accountingdate as gl_date,
            cast(upper(trim(vnd.dataareaid)) as varchar(255)) as company_code,
            cast(gj.journalnumber as varchar(255)) as batch_number,
            cast(vnd.transtype as varchar(255)) as batch_type,
            gj.accountingdate as batch_date,
            cast(
                case
                    when coalesce(vnd.amountcur, 0) < 0
                    then
                        case
                            when vnd.settleamountcur = vnd.amountcur
                            then 'PAID_IN_FULL'
                            else 'OPEN'
                        end
                    else ''
                end as varchar(255)
            ) as pay_status_code,
            cast(null as varchar(255)) as gl_offset_srccd,
            cast(
                case when gj.mainaccount is null then 'N' else 'P' end as varchar(6)
            ) as gl_posted_flag,
            cast(null as varchar(255)) as void_flag,
            cast(vnd.invoice as varchar(255)) as supplier_invoice_number,
            cast(vnd.txt as varchar(255)) as reference_txt,
            cast(null as varchar(255)) as remark_txt,
            cast(null as varchar(255)) as transaction_uom,
            cast(vnd.currencycode as varchar(255)) as txn_currency,
            vnd.amountcur as txn_gross_amt,
            vnd.amountcur - coalesce(vnd.settlecurtot, 0) as txn_open_amt,
            0 as txn_discount_available_amt,
            0 as txn_discount_taken_amt,
            case
                when coalesce(vij.sumtax, 0) <> 0 then vnd.amountcur else 0
            end as txn_taxable_amt,
            coalesce(vij.sumtax, 0) as txn_tax_amt,
            vnd.amountmst as base_gross_amt,
            vnd.amountmst - coalesce(vnd.settlemsttot, 0) as base_open_amt,
            0 as base_discount_available_amt,
            0 as base_discount_taken_amt,
            case
                when coalesce(vij.sumtax, 0) <> 0 then vnd.amountmst else 0
            end as base_taxable_amt,
            case
                when cast(vnd.amountcur as number(38, 10)) <> 0
                then
                    cast(vij.sumtax as number(38, 10))
                    * cast(vnd.amountmst as number(38, 10))
                    / cast(vnd.amountcur as number(38, 10))
                else 0
            end as base_tax_amt,
            cast('N' as varchar(1)) as intercompany_flag,
            ma.mainaccountid as source_object_code,
            vnd.modifieddatetime as source_updated_datetime
        from
            (
                select
                    upper(trim(v.dataareaid)) as dataareaid,
                    v.voucher as voucher,
                    max(v.accountnum) as accountnum,
                    max(v.paymmode) as paymmode,
                    max(vt.paymtermid) as paymtermid,
                    max(v.transdate) as transdate,
                    max(v.documentdate) as documentdate,
                    max(v.duedate) as duedate,
                    max(v.transtype) as transtype,
                    max(v.invoice) as invoice,
                    max(v.txt) as txt,
                    max(v.currencycode) as currencycode,
                    sum(v.amountcur) as amountcur,
                    sum(v.settleamountcur) as settleamountcur,
                    sum(v.amountmst) as amountmst,
                    sum(vstl.settlemsttot) as settlemsttot,
                    sum(vstl.gainloss) as gainloss,
                    sum(vstl.settlecurtot) as settlecurtot,
                    max(v.modifieddatetime) as modifieddatetime
                from vendtrans v
                inner join
                    vendtable vt
                    on v.accountnum = vt.accountnum
                    and upper(trim(v.dataareaid)) = upper(trim(vt.dataareaid))
                left outer join
                    (
                        select
                            transrecid,
                            sum(settleamountmst) - sum(exchadjustment) as settlemsttot,
                            sum(exchadjustment) as gainloss,
                            sum(settleamountcur) as settlecurtot
                        from vendsettlement
                        group by transrecid
                    ) vstl
                    on v.recid = vstl.transrecid
                where v.transtype not in (9, 15, 24)
                group by upper(trim(v.dataareaid)), v.voucher
            ) vnd
        left outer join
            vendinvoicejour vij
            on vnd.invoice = vij.invoiceid
            and vnd.voucher = vij.ledgervoucher
            and upper(trim(vnd.dataareaid)) = upper(trim(vij.dataareaid))
        inner join
            journal gj
            on trim(vnd.voucher) = trim(gj.subledgervoucher)
            and trim(upper(vnd.dataareaid)) = trim(upper(gj.subledgervoucherdataareaid))
            and vnd.transdate = gj.accountingdate
        left join mainaccount ma on ma.recid = gj.mainaccount
    ),

    src_qualifier2 as (
        select
            '{{ env_var("DBT_SOURCE_SYSTEM") }}' as source_system,
            cast(upper(trim(vnd.dataareaid)) as varchar(255)) as document_company,
            cast(vnd.voucher as varchar(255)) as document_number,
            cast('V_' || substr(vnd.voucher, 1, 3) as varchar(255)) as document_type,
            cast('-' as varchar(255)) as source_business_unit_code,
            cast(vnd.accountnum as varchar(255)) as source_supplier_identifier,
            cast(
                iff(
                    trim(vt.paymtermid) = '' or trim(vt.paymtermid) is null,
                    iff(
                        trim(vij.payment) = '' or trim(vij.payment) is null,
                        'IMM',
                        trim(vij.payment)
                    ),
                    trim(vt.paymtermid)
                ) as varchar(255)
            ) as source_payment_terms_code,
            cast(gj.mainaccount as varchar(255)) as source_account_identifier,
            cast(null as varchar(255)) as source_payee_identifier,
            cast(
                upper(trim(vnd.paymmode)) as varchar(255)
            ) as source_payment_instr_code,
            vnd.transdate as invoice_date,
            vnd.documentdate as voucher_date,
            vnd.duedate as net_due_date,
            null as discount_date,
            gj.accountingdate as gl_date,
            cast(upper(trim(vnd.dataareaid)) as varchar(255)) as company_code,
            cast(gj.journalnumber as varchar(255)) as batch_number,
            cast(vnd.transtype as varchar(255)) as batch_type,
            gj.accountingdate as batch_date,
            cast(
                case
                    when coalesce(vnd.amountcur, 0) < 0
                    then
                        case
                            when vnd.settleamountcur = vnd.amountcur
                            then 'PAID_IN_FULL'
                            else 'OPEN'
                        end
                    else ''
                end as varchar(255)
            ) as pay_status_code,
            cast(null as varchar(255)) as gl_offset_srccd,
            cast(
                case when gj.mainaccount is null then 'N' else 'P' end as varchar(6)
            ) as gl_posted_flag,
            cast(null as varchar(255)) as void_flag,
            cast(vnd.invoice as varchar(255)) as supplier_invoice_number,
            cast(vnd.txt as varchar(255)) as reference_txt,
            cast(null as varchar(255)) as remark_txt,
            cast(null as varchar(255)) as transaction_uom,
            cast(vnd.currencycode as varchar(255)) as txn_currency,
            vnd.amountcur as txn_gross_amt,
            vnd.amountcur - vnd.settleamountcur as txn_open_amt,
            0 as txn_discount_available_amt,
            0 as txn_discount_taken_amt,
            case
                when coalesce(vij.sumtax, 0) <> 0 then vnd.amountcur else 0
            end as txn_taxable_amt,
            coalesce(vij.sumtax, 0) as txn_tax_amt,
            vnd.amountmst as base_gross_amt,
            vnd.amountmst - vnd.settleamountmst as base_open_amt,
            0 as base_discount_available_amt,
            0 as base_discount_taken_amt,
            case
                when coalesce(vij.sumtax, 0) <> 0 then vnd.amountmst else 0
            end as base_taxable_amt,
            case
                when cast(vnd.amountcur as number(38, 10)) <> 0
                then
                    cast(vij.sumtax as number(38, 10))
                    * cast(vnd.amountmst as number(38, 10))
                    / cast(vnd.amountcur as number(38, 10))
                else 0
            end as base_tax_amt,
            cast('N' as varchar(1)) as intercompany_flag,
            ma.mainaccountid as source_object_code,
            vnd.modifieddatetime as source_updated_datetime
        from vendtrans vnd
        inner join
            vendtable vt
            on vnd.accountnum = vt.accountnum
            and upper(trim(vnd.dataareaid)) = upper(trim(vt.dataareaid))
        left outer join
            vendinvoicejour vij
            on vnd.invoice = vij.invoiceid
            and vnd.voucher = vij.ledgervoucher
            and upper(trim(vnd.dataareaid)) = upper(trim(vij.dataareaid))
        inner join
            journal gj
            on trim(vnd.voucher) = trim(gj.subledgervoucher)
            and trim(upper(vnd.dataareaid)) = trim(upper(gj.subledgervoucherdataareaid))
            and vnd.transdate = gj.accountingdate
        left join mainaccount ma on ma.recid = gj.mainaccount
        where
            vnd.transtype in (9, 15, 24)
            and vnd.recid not in (select transrecid from vendsettlement)
    )


select *
from src_qualifier1
union
select *
from src_qualifier2
