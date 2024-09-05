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

    custinvoicetrans as (select * from {{ ref("src_custinvoicetrans") }}),

    custtable as (select * from {{ ref("src_custtable") }}),

    custinvoicejour as (select * from {{ ref("src_custinvoicejour") }}),

    custsettlement as (select * from {{ ref("src_custsettlement") }}),

    gje as (select * from {{ ref("src_generaljournalentry") }}),

    gjae as (select * from {{ ref("src_generaljournalaccountentry") }}),

    inventdim as (select * from {{ ref("src_inventdim") }}),

    mainaccount as (select * from {{ ref("src_mainaccount") }}),

    salestable as (select * from {{ ref("src_salestable") }}),

    salesline as (
        select dataareaid, salesid, itemid, salestype, max(linenum) as linenum
        from {{ ref("src_salesline") }}
        where salesstatus <> '4'
        group by dataareaid, salesid, itemid, salestype
    ),

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
    )

select
    '{{ env_var("DBT_SOURCE_SYSTEM") }}' as source_system,
    cast(ctr.voucher as varchar(255)) as document_number,
    cast('C_' || substr(ctr.voucher, 1, 3) as varchar(255)) as document_type,
    cast(upper(trim(ctr.dataareaid)) as varchar(255)) as document_company,
    cast(
        to_char(trim(ctr.invoice))
        || '.'
        || to_char(to_number(cit.linenum))
        || '.'
        || to_char(cit.recid) as varchar(255)
    ) as document_pay_item,
    cast(ctr.accountnum as varchar(255)) as source_customer_identifier,
    ctr.transdate as gl_date,
    cit.invoicedate as invoice_date,
    cast(ctr.transtype as varchar(255)) as batch_type,
    cast(gj.journalnumber as varchar(255)) as batch_number,
    gj.documentdate as batch_date,
    cast(upper(trim(ctr.dataareaid)) as varchar(255)) as company_code,
    cast(null as varchar(255)) as gl_offset_srccd,
    cast(gj.mainaccount as varchar(255)) as source_account_identifier,
    cast(null as varchar(255)) as source_payor_identifier,
    cast(
        case when gj.mainaccount is null then 'N' else 'P' end as varchar(6)
    ) as gl_posted_flag,
    cast(
        case
            when ctr.closed <> '1900-01-01' and ctr.amountcur = ctr.settleamountcur
            then 'PAID_IN_FULL'
            else 'OPEN'
        end as varchar(255)
    ) as pay_status_code,
    cast(
        case when cit.taxgroup = 'DOM' then 'D' else 'F' end as varchar(255)
    ) as foreign_transaction_flag,
    ctr.transdate as service_date,
    cast(
        coalesce(trim(id.inventlocationid), '-') as varchar(255)
    ) as source_business_unit_code,
    cast(
        coalesce(trim(cij.payment), trim(ct.paymtermid)) as varchar(255)
    ) as source_payment_terms_code,
    ctr.duedate as net_due_date,
    cast(null as varchar(255)) as original_document_number,
    cast(null as varchar(255)) as original_document_type,
    cast(null as varchar(255)) as original_document_company,
    cast(null as varchar(255)) as original_document_pay_item,
    cast(null as varchar(255)) as supplier_invoice_number,
    cast(ctr.invoice as varchar(255)) as sales_document_number,
    cast(sl.salestype as varchar(255)) as sales_document_type,
    cast(upper(trim(sl.dataareaid)) as varchar(255)) as sales_document_company,
    cast(to_number(sl.linenum, 38, 1) as varchar(255)) as sales_document_suffix,
    ctr.closed as cleared_date,
    cast(ctr.txt as varchar(255)) as reference_txt,
    cast(cit.lineheader as varchar(255)) as remark_txt,
    cast(null as varchar(255)) as name_txt,
    cast(cit.itemid as varchar(255)) as source_item_identifier,
    cit.qty as quantity,
    case
        when (trim(cit.salesunit)) IS null or (trim(cit.salesunit)) = ''
        then '-'
        else upper(trim(cit.salesunit))
    end as transaction_uom,
    cast(ctr.paymmode as varchar(255)) as source_payment_instr_code,
    cast(null as datetime) as void_date,
    cast(null as varchar(255)) as void_flag,
    cast(ctr.lastsettlevoucher as varchar(255)) as payment_identifier,
    ctr.closed as invoice_closed_date,
    cast(null as varchar(255)) as deduction_reason_code,
    cast(ctr.currencycode as varchar(255)) as txn_currency,
    cit.lineamount as txn_gross_amt,
    case when ctr.closed = '1900-01-01' then cit.lineamount else 0 end as txn_open_amt,
    coalesce(cit.sumlinedisc, 0) as txn_discount_available_amt,
    coalesce(cit.sumlinedisc, 0) as txn_discount_taken_amt,
    case
        when coalesce(cij.sumtax, 0) <> 0 then cit.lineamount else 0
    end as txn_taxable_amt,
    case
        when coalesce(cij.sumtax, 0) = 0 then cit.lineamount else 0
    end as txn_nontaxable_amt,
    case
        when coalesce(cij.sumtax, 0) <> 0
        then
            case
                when coalesce(ctr.amountcur, 0) - coalesce(cij.sumtax, 0) <> 0
                then
                    (
                        cast(cit.lineamount as decimal(38, 10))
                        * cast(coalesce(cij.sumtax, 0) as number(38, 10))
                        / (
                            cast(ctr.amountcur as decimal(38, 10))
                            - cast(coalesce(cij.sumtax, 0) as number(38, 10))
                        )
                    )
                else 0
            end
        else 0
    end as txn_tax_amt,
    cit.lineamountmst as base_gross_amt,
    case
        when ctr.closed = '1900-01-01' then cit.lineamountmst else 0
    end as base_open_amt,
    coalesce(cit.sumlinediscmst, 0) as base_discount_available_amt,
    coalesce(cit.sumlinediscmst, 0) as base_discount_taken_amt,
    case
        when coalesce(cij.sumtaxmst, 0) <> 0 then cit.lineamountmst else 0
    end as base_taxable_amt,
    case
        when coalesce(cij.sumtaxmst, 0) = 0 then cit.lineamountmst else 0
    end as base_nontaxable_amt,
    case
        when coalesce(cij.sumtaxmst, 0) <> 0
        then
            case
                when coalesce(ctr.amountmst, 0) - cij.sumtaxmst <> 0
                then
                    cast(cij.sumtaxmst as decimal(38, 10))
                    * cit.lineamountmst
                    / (
                        cast(ctr.amountmst as decimal(38, 10))
                        - coalesce(cast(cij.sumtaxmst as decimal(38, 10)), 0)
                    )
                else 0
            end
        else 0
    end as base_tax_amt,
    cast('N' as varchar(1)) as intercompany_flag,
    ma.mainaccountid as source_object_code,
    case
        when ctr.modifieddatetime > cit.modifieddatetime
        then ctr.modifieddatetime
        else cit.modifieddatetime
    end as source_updated_datetime
from custtrans ctr
inner join
    custinvoicetrans cit
    on ctr.invoice = cit.invoiceid
    and upper(trim(ctr.dataareaid)) = upper(trim(cit.dataareaid))
    and case when cit.parentrecid <> 0 then ctr.recid else 0 end = cit.parentrecid
    and cit.invoiceid <> ''
inner join
    custtable ct
    on ct.accountnum = ctr.accountnum
    and upper(trim(ctr.dataareaid)) = upper(trim(ct.dataareaid))
left outer join
    custinvoicejour cij
    on ctr.invoice = cij.invoiceid
    and ctr.voucher = cij.ledgervoucher
    and upper(trim(ctr.dataareaid)) = upper(trim(cij.dataareaid))
left outer join
    inventdim id
    on cit.inventdimid = id.inventdimid
    -- BELOW JOIN RETRIEVES AR ACCOUNT
    and upper(trim(cit.dataareaid)) = upper(trim(id.dataareaid))
left outer join
    journal gj
    on trim(ctr.voucher) = trim(gj.subledgervoucher)
    and trim(upper(ctr.dataareaid)) = trim(upper(gj.subledgervoucherdataareaid))
left join mainaccount ma on ma.recid = gj.mainaccount
left join
    salestable st
    on cij.salesid = st.salesid
    and trim(upper(cij.dataareaid)) = trim(upper(st.dataareaid))
left outer join
    salesline sl
    on upper(trim(sl.dataareaid)) = upper(trim(cit.dataareaid))
    and sl.salesid = cit.origsalesid
    and sl.itemid = cit.itemid
where trim(ctr.invoice) is not null and ctr.transtype not in (9, 15, 27)
