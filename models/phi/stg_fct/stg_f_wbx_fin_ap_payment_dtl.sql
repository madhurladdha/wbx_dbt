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

    vendtransfilter as (
        select distinct
            upper(trim(dataareaid)) as company,
            trim(voucher) as payvoucher,
            currencycode,
            paymmode
        from vendtrans
        where transtype in (9, 15, 24)
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
    vendinvoicetrans as (select * from {{ ref("src_vendinvoicetrans") }}),
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
    vndp.payvoucher as payment_identifier,
    row_number() over (
        partition by upper(trim(vndi.dataareaid)), vndp.payvoucher order by vndi.recid
    ) as line_number,
    upper(trim(vndi.dataareaid)) as document_company,
    upper(trim(vndi.dataareaid)) as company_code,
    vndi.voucher as document_number,
    'V_' || substr(vndi.voucher, 1, 3) as document_type,
        cast(
        to_char(trim(vndi.invoice))
        || '.'
        || to_char(to_number(vit.linenum))
        || '.'
        || to_char(vit.recid) as varchar(255)
    ) as document_pay_item,
    cast(to_number(vit.linenum) as varchar(255)) as document_pay_item_ext,
    /*
    --updates to resolve join issue between invoice & payments using nature key
    vndi.invoice as document_pay_item,
    cast(null as varchar(255)) as document_pay_item_ext,*/
    '-' as source_business_unit_code,
    cast(null as varchar(255)) as source_payee_identifier,
    cast(gj.mainaccount as varchar(255)) as source_account_identifier,
    upper(trim(vndp.paymmode)) as source_payment_instr_code,
    case when gj.mainaccount is null then 'N' else 'P' end as gl_posted_flag,
    cast(null as varchar(255)) as purchase_order_number,
    vndi.txt as remark_txt,
    case
        when vndi.documentnum = '' then null else vndi.documentnum
    end as payment_trans_doc_number,
    vndi.transdate as payment_trans_date,
    cast(null as date) as void_date,
    vndi.closed as payment_trans_cleared_date,
    gj.journalnumber as batch_number,
    to_char(to_number(vndi.transtype)) as batch_type,
    gj.accountingdate as batch_date,
    cast(null as varchar(255)) as void_flag,
    vndp.currencycode as txn_currency,
    vstl.settleamountcur as txn_payment_amt,
    0 as txn_discount_available_amt,
    0 as txn_discount_taken_amt,
    vstl.settleamountmst as base_payment_amt,
    0 as base_discount_available_amt,
    0 as base_discount_taken_amt,
    'N' as intercompany_flag,
    ma.mainaccountid as source_object_code,
    vndi.modifieddatetime as source_updated_datetime
from
    vendtransfilter vndp
inner join
    vendsettlement vstl
    on trim(vndp.payvoucher) = trim(vstl.offsettransvoucher)
    and vndp.company = upper(trim(vstl.dataareaid))
inner join
    vendtrans vndi
    on vndi.recid = vstl.transrecid
    and upper(trim(vndi.dataareaid)) = upper(trim(vstl.dataareaid))
left join
    vendtable vt
    on upper(trim(vndi.dataareaid)) = upper(trim(vt.dataareaid))
    and vndi.accountnum = vt.accountnum
left outer join
    vendinvoicejour vij
    on vndi.invoice = vij.invoiceid
    and vndi.voucher = vij.ledgervoucher
    and upper(trim(vndi.dataareaid)) = upper(trim(vij.dataareaid))
left outer join
    journal gj
    on trim(vndi.voucher) = trim(gj.subledgervoucher)
    and trim(upper(vndi.dataareaid)) = trim(upper(gj.subledgervoucherdataareaid))
left join mainaccount ma on ma.recid = gj.mainaccount
-- ADDING BELOW TO RESOLVE ISSUES WITH JOINING TO INVOICE
left join
    vendinvoicetrans vit
    on vndi.invoice = vit.invoiceid
    and upper(trim(vndi.dataareaid)) = upper(trim(vit.dataareaid))
    and vit.invoicedate = vndi.transdate
-- ADDING BELOW TO FILTER OUT CURRENCY ADJUSTMENT REVERSING ENTRIES
where vndi.voucher not like 'EXS%' and vndi.voucher not like 'FXV%'
