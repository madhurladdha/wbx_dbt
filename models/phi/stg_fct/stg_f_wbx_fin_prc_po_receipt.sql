{{ config(tags=["finance", "po"]) }}

{% set now = modules.datetime.datetime.now() %}
{%- set full_load_day -%} {{env_var('DBT_FULL_LOAD_DAY')}} {%- endset -%}
{%- set day_today -%} {{ now.strftime('%A') }} {%- endset -%}

with
    purchparmtable as (
        select *
        from {{ ref("src_purchparmtable") }}

        /*  Removing incremental logic until it can be fixed for the correct table to filter.  7/13/2023.
    */
    ),

    purchtable as (select * from {{ ref("src_purchtable") }}),

    purchline as (select * from {{ ref("src_purchline") }}),

    purchparmline as (select * from {{ ref("src_purchparmline") }}),

    inventdim as (select * from {{ ref("src_inventdim") }}),

    accountingdistribution as (select * from {{ ref("src_accountingdistribution") }}),

    dimensionattributevaluecombo as (
        select * from {{ ref("src_dimensionattributevaluecombo") }}
    ),

    mainaccount as (select * from {{ ref("src_mainaccount") }}),

    dimensionattributelevelvalueview as (
        select * from {{ ref("src_dimensionattributelevelvalueview") }}
    ),

    dimensionattribute as (select * from {{ ref("src_dimensionattribute") }}),

    dimensionjoincc as (
        select dalvv.displayvalue, dalvv.valuecombinationrecid, da.name, dalvv.partition
        from dimensionattributelevelvalueview dalvv
        inner join dimensionattribute da on dalvv.dimensionattribute = da.recid
        where da.name = 'CostCenters'
    ),

    accountingdistributionjoin as (
        select
            nvl(cc.displayvalue, '-') as cost_center,
            ad.ledgerdimension,
            ad.transactioncurrency,
            ad.transactioncurrencyamount,
            ad.sourcedocumentheader,
            ad.sourcedocumentline,
            ad.accountingdate,
            davc.displayvalue,
            davc.mainaccount,
            ma.mainaccountid,
            ad.number_
        from accountingdistribution ad
        inner join dimensionattributevaluecombo davc on ad.ledgerdimension = davc.recid
        inner join mainaccount ma on davc.mainaccount = ma.recid
        left outer join
            dimensionjoincc cc on ad.ledgerdimension = cc.valuecombinationrecid
        where ad.number_ = 1
    ),

    purchparmjoin as (
        select distinct
            ppl.origpurchid,
            ppl.purchaselinelinenumber,
            ppt.num,
            date_trunc('month', ppt.transdate) transdate_month,
            sum(ppl.remainafter) as remainafter,
            sum(ppl.remainbefore) as remainbefore,
            sum(ppl.receivenow) as receivenow
        from purchparmtable ppt
        inner join
            purchparmline ppl
            on ppt.tablerefid = ppl.tablerefid
            and ppt.parmid = ppl.parmid
            and ppt.enddatetime <> '1900-01-01'
        group by 1, 2, 3, 4
    ),

    agreementheader as (select * from {{ ref("src_agreementheader") }})


select distinct
    '{{ env_var("DBT_SOURCE_SYSTEM") }}' as source_system,
    cast(pt.purchid as varchar2(255)) as po_order_number,
    cast(pt.purchasetype as varchar2(255)) as po_order_type,
    '1' as po_receipt_match_type,
    0 as po_number_of_lines,
    cast(pl.linenumber as number(38,10)) as po_line_number,
    cast('-' as varchar2(255)) as po_order_suffix,
    cast(upper(trim(pt.dataareaid)) as varchar2(255)) as po_order_company,
    cast(
        case when rcpts.num = '' then ' ' else rcpts.num end as varchar2(255)
    ) as document_number,
    case
        when trim(pl.itemid) = '' or trim(pl.itemid) is null then 'N' else 'S'
    end as line_type,
    'RECEIPT' as document_type,
    cast(upper(trim(rcpts.dataareaid)) as varchar2(255)) as document_company,
    cast(
        (rcpts.parmid || '.' || to_number(rcpts.linenum)) as varchar2(255)
    ) as document_pay_item,
    case
        when id.inventlocationid = ''
        then ' '
        else cast(upper(id.inventlocationid) as varchar2(255))
    end as source_business_unit_code,
    cast(pt.invoiceaccount as varchar2(255)) as source_supplier_identifier,
    case
        when pt.payment = '' then ' ' else cast(pt.payment as varchar2(255))
    end as source_payment_terms_code,
    cast(adj.mainaccount as varchar2(255)) as source_account_identifier,
    case
        when rcpts.itemid = '' then ' ' else cast(rcpts.itemid as varchar2(255))
    end as source_item_identifier,
    cast(null as string(255)) as source_subledger_identifier,
    cast(null as string(255)) as source_subledger_type,
    case
        when rcpts.remainafter = 0 then 'PAID_IN_FULL' else 'OPEN'
    end as pay_status_code,
    (
        case
            when rcpts.parmjobstatus = 1 and trim(rcpts.log) like '1#Posting%'
            then '4'
            else cast(pl.purchstatus as varchar(255))
        end
    ) as line_status,
    cast(
        to_char(to_date(substr(pt.createddatetime, 1, 10), 'YYYY-MM-DD')) as varchar2(
            255
        )
    ) as po_order_date,
    cast(
        to_char(to_date(substr(rcpts.transdate, 1, 10), 'YYYY-MM-DD')) as varchar2(255)
    ) as po_received_date,
    cast(
        to_char(to_date(substr(pl.deliverydate, 1, 10), 'YYYY-MM-DD')) as varchar2(255)
    ) as po_requested_date,
    cast(
        to_char(to_date(substr(pl.confirmeddlv, 1, 10), 'YYYY-MM-DD')) as varchar2(255)
    ) as po_promised_dlv_date,
    cast(
        to_char(to_date(substr(pt.accountingdate, 1, 10), 'YYYY-MM-DD')) as varchar2(
            255
        )
    ) as po_gl_date,
    case
        when pl.purchunit = '' then ' ' else cast(upper(pl.purchunit) as varchar2(255))
    end as transaction_uom,
    cast(pl.purchqty as string(255)) as receipt_order_quantity,
    cast(
        (pl.purchqty - rcpts.remainafter) as string(255)
    ) as receipt_paidtodate_quantity,
    cast(rcpts.remainafter as string(255)) as receipt_open_quantity,
    cast(rcpts.receivenow as string(255)) as receipt_received_quantity,
    cast(0 as string(255)) as receipt_closed_quantity,
    cast(0 as string(255)) as receipt_stocked_quantity,
    cast(0 as string(255)) as receipt_returned_quantity,
    cast(0 as string(255)) as receipt_reworked_quantity,
    cast(0 as string(255)) as receipt_scrapped_quantity,
    cast(0 as string(255)) as receipt_rejected_quantity,
    cast(0 as string(255)) as receipt_adjusted_quantity,
    cast(
        case
            when rcpts.priceunit <> 0
            then rcpts.purchprice / rcpts.priceunit
            else rcpts.purchprice
        end as string(255)
    ) as receipt_unit_cost,
    cast(trim(rcpts.currencycode) as varchar2(255)) as transaction_currency,
    cast(
        cast((pl.purchqty - rcpts.remainafter) as number(32, 10)) * (
            case
                when rcpts.priceunit <> 0
                then rcpts.purchprice / rcpts.priceunit
                else rcpts.purchprice
            end
        ) as string(255)
    ) as receipt_paidtodate_amt,
    cast(
        cast((rcpts.remainafter) as number(32, 10)) * (
            case
                when rcpts.priceunit <> 0
                then rcpts.purchprice / rcpts.priceunit
                else rcpts.purchprice
            end
        ) as string(255)
    ) as receipt_open_amt,
    cast(
        cast((rcpts.receivenow) as number(32, 10)) * (
            case
                when rcpts.priceunit <> 0
                then rcpts.purchprice / rcpts.priceunit
                else rcpts.purchprice
            end
        ) as string(255)
    ) as receipt_received_amt,
    cast(0 as string(255)) as receipt_closed_amt,
    cast(null as string(255)) as supplier_invoice_number,
    cast(null as string(255)) as gl_offset_srccd,
    cast(
        to_char(to_date(substr(rcpts.transdate, 1, 10), 'YYYY-MM-DD')) as varchar2(255)
    ) as source_date_updated,
    cast(trim(pt.returnitemnum) as varchar2(255)) as rma_number,
    cast(nvl(trim(pa.purchnumbersequence), '-') as varchar2(255)) as agreement_number,
    adj.cost_center as source_cost_center,
    adj.mainaccountid as source_object_code
from purchtable pt
inner join
    purchline pl
    on upper(trim(pt.dataareaid)) = upper(trim(pl.dataareaid))
    and pt.purchid = pl.purchid
inner join
    inventdim id
    on upper(trim(pt.dataareaid)) = upper(trim(pl.dataareaid))
    and pl.inventdimid = id.inventdimid
    -- Condition added to remove duplicate on 24 Nov 2020
    and upper(trim(id.dataareaid)) = upper(trim(pt.dataareaid))

inner join
    (
        select distinct
            ppl.dataareaid,
            ppl.purchlinerecid,
            ppl.origpurchid,
            ppl.purchaselinelinenumber,
            ppt.purchid,
            ppt.ordering,
            first_value(ppt.transdate) over (
                partition by
                    ppl.origpurchid,
                    ppl.purchaselinelinenumber,
                    ppt.num,
                    date_trunc('month', ppt.transdate)
                order by ppt.createddatetime asc
            ) as transdate,
            ppt.num,
            ppt.purchname,
            ppt.orderaccount,
            ppt.currencycode,
            qty.remainafter as remainafter,
            qty.remainbefore as remainbefore,
            qty.receivenow as receivenow,
            ppl.linenum,
            ppl.itemid,
            first_value(ppl.priceunit) over (
                partition by
                    ppl.origpurchid,
                    ppl.purchaselinelinenumber,
                    ppt.num,
                    date_trunc('month', ppt.transdate)
                order by ppt.createddatetime asc
            ) as priceunit,
            first_value(ppl.purchprice) over (
                partition by
                    ppl.origpurchid,
                    ppl.purchaselinelinenumber,
                    ppt.num,
                    date_trunc('month', ppt.transdate)
                order by ppt.createddatetime asc
            ) as purchprice,
            last_value(ppl.lineamount) over (
                partition by
                    ppl.origpurchid,
                    ppl.purchaselinelinenumber,
                    ppt.num,
                    date_trunc('month', ppt.transdate)
                order by ppt.createddatetime
                rows between current row and unbounded following
            ) as lineamount,
            last_value(ppt.parmid) over (
                partition by
                    ppl.origpurchid,
                    ppl.purchaselinelinenumber,
                    ppt.num,
                    date_trunc('month', ppt.transdate)
                order by ppt.createddatetime
                rows between current row and unbounded following
            ) as parmid,
            ppt.parmjobstatus,
            ppt.log
        from purchparmtable ppt
        inner join
            purchparmline ppl
            on ppt.tablerefid = ppl.tablerefid
            and ppt.parmid = ppl.parmid
            and ppt.enddatetime <> '1900-01-01'
        inner join
            purchparmjoin qty
            on qty.origpurchid = ppl.origpurchid
            and qty.purchaselinelinenumber = ppl.purchaselinelinenumber
            and ppt.num = qty.num
            and date_trunc('month', ppt.transdate) = qty.transdate_month
    ) rcpts
    on pl.purchid = rcpts.origpurchid
    and pl.linenumber = rcpts.purchaselinelinenumber
-- --This sub-select captures the SOURCE_ACCOUNT_IDENTIFER and SOURCE_COST_CENTER
-- which are required for proper lookup on FIN_ACCOUNT_DIM
-- --Adjusted the join for the Cost Center to be an outer join as not all accounts
-- that need to be assigned have a real Cost Center.
left outer join
    accountingdistributionjoin adj
    on pt.sourcedocumentheader = adj.sourcedocumentheader
    and pl.sourcedocumentline = adj.sourcedocumentline
left outer join agreementheader pa on pt.matchingagreement = pa.recid
