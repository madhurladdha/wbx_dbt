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

    vendinvoicetrans as (
        select * from {{ ref("src_vendinvoicetrans") }}
    ),

    inventdim as (select * from {{ ref("src_inventdim") }}),

    generaljournalentry as (select * from {{ ref("src_generaljournalentry") }}),

    generaljournalaccountentry as (
        select * from {{ ref("src_generaljournalaccountentry") }}
    ),

    mainaccount as (select * from {{ ref("src_mainaccount") }}),

    journal as (
        select
            gje.subledgervoucher,
            max(gje.accountingdate) as accountingdate,
            gje.subledgervoucherdataareaid,
            min(gjae.mainaccount) as mainaccount,
            max(gje.journalnumber) as journalnumber
        from generaljournalentry gje, generaljournalaccountentry gjae
        where gje.recid = gjae.generaljournalentry and gjae.postingtype in (41, 48)
        group by gje.subledgervoucher, gje.subledgervoucherdataareaid
    )

select
    '{{ env_var("DBT_SOURCE_SYSTEM") }}' as source_system,
    cast(upper(trim(vnd.dataareaid)) as varchar(255)) as document_company,
    cast(vnd.voucher as varchar(255)) as document_number,
    cast('V_' || substr(vnd.voucher, 1, 3) as varchar(255)) as document_type,
    cast(
        to_char(trim(vnd.invoice))
        || '.'
        || to_char(to_number(vit.linenum))
        || '.'
        || to_char(vit.recid) as varchar(255)
    ) as document_pay_item,
    cast(to_number(vit.linenum) as varchar(255)) as document_pay_item_ext,
    cast('-' as varchar(255)) as source_business_unit_code,
    cast(vnd.accountnum as varchar(255)) as source_supplier_identifier,
    cast(
        case
            when trim(vt.paymtermid) = '' or trim(vt.paymtermid) is null
            then 'IMM'
            else trim(vt.paymtermid)
        end as varchar(255)
    ) as source_payment_terms_code,
    cast(gj.mainaccount as varchar(255)) as source_account_identifier,
    cast(null as varchar(255)) as source_payee_identifier,
    cast(vit.itemid as varchar(255)) as source_item_identifier,
    cast(upper(trim(vnd.paymmode)) as varchar(255)) as source_payment_instr_code,
    coalesce(vit.invoicedate, vnd.transdate) as invoice_date,
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
    cast(substr(vit.name, 1, 255) as varchar(255)) as remark_txt,
    vit.qty as quantity,
    cast(upper(trim(vit.purchunit)) as varchar(255)) as transaction_uom,
    cast(vnd.currencycode as varchar(255)) as txn_currency,
    vit.lineamount as txn_gross_amt,
    case when vnd.closed = '1900-01-01' then vit.lineamount else 0 end as txn_open_amt,
    0 as txn_discount_available_amt,
    0 as txn_discount_taken_amt,
    case
        when coalesce(vit.taxamount, 0) <> 0 then vit.lineamount else 0
    end as txn_taxable_amt,
    coalesce(vit.taxamount, 0) as txn_tax_amt,
    vit.lineamountmst as base_gross_amt,
    case
        when vnd.closed = '1900-01-01' then vit.lineamountmst else 0
    end as base_open_amt,
    0 as base_discount_available_amt,
    0 as base_discount_taken_amt,
    case
        when coalesce(vit.taxamount, 0) <> 0 then vit.lineamountmst
    end as base_taxable_amt,
    case
        when vnd.amountcur <> 0
        then
            cast(coalesce(vit.taxamount, 0) as number(38, 10))
            * cast(vnd.amountmst as number(38, 10))
            / cast(vnd.amountcur as number(38, 10))
        else 0
    end as base_tax_amt,
    cast('N' as varchar(1)) as intercompany_flag,
    ma.mainaccountid as source_object_code,
    case
        when vnd.modifieddatetime > vit.modifieddatetime
        then vnd.modifieddatetime
        else vit.modifieddatetime
    end as source_updated_datetime,systimestamp() as load_date
    ,vit.purchid as po_order_number
    ,vnd.approver as approver
from
    (
        select
            upper(trim(v.dataareaid)) as dataareaid,
            v.voucher as voucher,
            max(v.invoice) as invoice,
            max(v.accountnum) as accountnum,
            max(v.paymmode) as paymmode,
            max(v.transdate) as transdate,
            max(v.documentdate) as documentdate,
            max(v.duedate) as duedate,
            max(v.transtype) as transtype,
            sum(v.amountcur) as amountcur,
            sum(v.settleamountcur) as settleamountcur,
            max(v.txt) as txt,
            max(v.currencycode) as currencycode,
            max(v.closed) as closed,
            sum(v.amountmst) as amountmst,
            max(v.modifieddatetime) as modifieddatetime,
            max(v.approver) as approver
        from vendtrans v
        group by upper(trim(v.dataareaid)), v.voucher
    ) vnd
inner join
    vendinvoicetrans vit
    on vnd.invoice = vit.invoiceid
    and upper(trim(vnd.dataareaid)) = upper(trim(vit.dataareaid))
    and vit.invoicedate = vnd.transdate
left join
    vendtable vt
    on upper(trim(vnd.dataareaid)) = upper(trim(vt.dataareaid))
    and vnd.accountnum = vt.accountnum
left outer join
    inventdim id
    on vit.inventdimid = id.inventdimid
    and upper(trim(vit.dataareaid)) = upper(trim(id.dataareaid))
left outer join
    journal gj
    on trim(vnd.voucher) = trim(gj.subledgervoucher)
    and trim(upper(vnd.dataareaid)) = trim(upper(gj.subledgervoucherdataareaid))
left join mainaccount ma on ma.recid = gj.mainaccount
where trim(vnd.invoice) is not null and vnd.transtype not in (9, 15, 24)
