{{ config(
    tags=["wbx", "sales","actuals","sales_actuals"],
    snowflake_warehouse= env_var("DBT_WBX_SF_WH"),
    ) 
}}
with salesline as (
    select
        *,
        sum(salesline.qtyordered) over (
            partition by dataareaid, salesid, itemid, linenum
        ) as qtyordered_line,
        sum(salesline.salesqty) over (
            partition by dataareaid, salesid, itemid, linenum
        ) as salesqty_line,
        sum(salesline.lineamount) over (
            partition by dataareaid, salesid, itemid, linenum
        ) as lineamount_line
    from {{ ref('src_salesline') }} as salesline
),

salestable as (
    select * from {{ ref('src_salestable') }}
),

custinvoicejour as (
   select * from {{ ref('src_custinvoicejour') }}
),

custpackingsliptrans as (
    select * from {{ ref('src_custpackingsliptrans') }}
),

lkp_custinvoicejour as (
     select salesid, dataareaid, partition, max(invoicedate) as invoicedate
    from custinvoicejour
    group by 1, 2, 3
),

lkp_custpackingsliptrans as (
     select
        *,
        sum(custpackingsliptrans.qty) over (
            partition by origsalesid, inventtransid, dataareaid
        ) as qty_sum,
        sum(custpackingsliptrans.amt) over (
            partition by origsalesid, inventtransid, dataareaid
        ) as amt_sum
    from
        (
            select
                origsalesid,
                inventtransid,
                deliverydate,
                dataareaid,
                sum(qty) as qty,
                sum(valuemst) as amt
            from custpackingsliptrans
            group by origsalesid, inventtransid, deliverydate, dataareaid
        ) as custpackingsliptrans
),

custinvoicetrans as (
    select * from {{ ref('src_custinvoicetrans') }}
),

dimensionattributelevelvalueview as (
    select * from {{ ref('src_dimensionattributelevelvalueview') }}
),

dimensionattribute as (
    select * from {{ ref('src_dimensionattribute') }}
),

inventdim as (
    select * from {{ ref('src_inventdim') }}
),

lkp_custinvoicetrans as (
    select
        *,
        sum(custinvoicetrans.qty) over (
            partition by origsalesid, inventtransid, dataareaid
        ) as qty_sum
    from
        (
            select
                cit.origsalesid,
                cit.inventtransid,
                cit.dataareaid,
                cit.invoicedate,
                ma.displayvalue as source_object_id,
                id.inventsizeid as variant_code,
                sum(cit.qty) as qty,
                sum(cit.lineamount + cit.sumlinedisc) as trans_invoice_grs_amt,
                sum(cit.lineamount) as trans_invoice_net_amt,
                sum(cit.sumlinedisc) as trans_invoice_disc_amt,
                sum(cit.lineamountmst + cit.sumlinediscmst) as base_invoice_grs_amt,
                sum(cit.lineamountmst) as base_invoice_net_amt,
                sum(cit.sumlinediscmst) as base_invoice_disc_amt
            from custinvoicetrans cit
            inner join
                custinvoicejour cij
                on cit.salesid = cij.salesid
                and cit.invoiceid = cij.invoiceid
                and cit.invoicedate = cij.invoicedate
                and cit.numbersequencegroup = cij.numbersequencegroup
                and cij.proforma <> 1
            -- ----Get the Account (object id)
            left join
                (
                    select
                        dalvv.displayvalue,
                        dalvv.valuecombinationrecid,
                        da.name,
                        dalvv.partition
                    from dimensionattributelevelvalueview dalvv
                    inner join
                        dimensionattribute da
                        on dalvv.dimensionattribute = da.recid
                    where da.name = 'MainAccount'
                ) ma
                on cit.partition = ma.partition
                and cit.ledgerdimension = ma.valuecombinationrecid
            left join
                inventdim id
                on id.inventdimid = cit.inventdimid
                and id.dataareaid = cit.dataareaid
            group by
                cit.origsalesid,
                cit.inventtransid,
                cit.dataareaid,
                cit.invoicedate,
                ma.displayvalue,
                id.inventsizeid
        ) custinvoicetrans
    where origsalesid <> ''
),

custconfirmtrans as (
    select * from {{ ref('src_custconfirmtrans') }}
),

custconfirmjour as (
    select * from {{ ref('src_custconfirmjour') }}
),

lkp_custconfirmtrans as (
    select 
        *
    from
        (
            select
                a.inventtransid,
                a.salesid,
                a.itemid,
                a.currencycode,
                a.salesunit,
                a.qty,
                a.salesprice,
                a.lineamount,
                a.inventqty,
                a.confirmdate,
                a.dataareaid,
                a.dlvdate,
                row_number() over (
                    partition by a.salesid, a.itemid, a.dataareaid
                    order by b.confirmdocnum
                ) as row_num
            from custconfirmtrans a
            inner join
                custconfirmjour b
                on a.salesid = b.salesid
                and a.confirmid = b.confirmid
                and a.confirmdate = b.confirmdate
        )
    where row_num = 1
),

wbxcusttableext as (
    select * from {{ ref('src_wbxcusttableext') }}
),

wbxsocancelreasontable as (
    select * from {{ ref('src_wbxsocancelreasontable') }}
),

dimensionattributevaluesetitem as (
    select * from {{ ref('src_dimensionattributevaluesetitem') }}
),

dimensionattributevalue as (
    select * from {{ ref('src_dimensionattributevalue') }}
),

dimensionattribute as (
    select * from {{ ref('src_dimensionattribute') }}
),

lkp_dimensionattributevaluesetitem as (
     select
            davs.partition, davs.dimensionattributevalueset, davs.displayvalue, da.name
        from dimensionattributevaluesetitem davs
        inner join
            dimensionattributevalue dav
            on davs.partition = dav.partition
            and davs.dimensionattributevalue = dav.recid
        inner join
            dimensionattribute da
            on dav.partition = da.partition
            and dav.dimensionattribute = da.recid
        where da.name in ('CostCenters')
),

despatched_date_max as (
    select origsalesid, dataareaid, max(deliverydate) as deliverydate
    from  custpackingsliptrans
    group by origsalesid, dataareaid

),

invoiced_date_max as (
    select origsalesid, dataareaid, max(invoicedate) as invoicedate
    from custinvoicetrans
    group by origsalesid, dataareaid
),

wmspickingroute as (
    select * from {{ ref('src_wmspickingroute') }}
),

lkp_wmspickingroute as (
    select a.transrefid, a.activationdatetime, a.dataareaid, a.pickingrouteid
        from wmspickingroute a
        where
            a.expeditionstatus <> 20
            and a.transtype = 0
            and a.activationdatetime = (
                select max(a1.activationdatetime)
                from wmspickingroute a1
                where
                    a.transrefid = a1.transrefid
                    and a.dataareaid = a1.dataareaid
                    and a1.expeditionstatus <> 20
                    and a1.transtype = 0
            )
)

select
    'WEETABIX' as source_system,
    trunc(salesline.linenum, 2)
    || '-'
    || trim(salesline.itemid)
    || '-'
    || trim(salesline.recid) as sales_line_number,
    salesline.salesid as sales_order_number,
    salesline.salestype as source_sales_order_type,
    upper(trim(salesline.dataareaid)) as sales_order_company,
    salesline.createdby as source_employee_code,
    case when salesline.stockedproduct <> 1 then 2 else 1 end as source_line_type_code,
    salestable.inventlocationid as ship_source_business_unit_code,
    upper(salestable.inventlocationid) as source_business_unit_code,
    salesline.custaccount as ship_source_customer_code,
    salestable.invoiceaccount as bill_source_customer_code,
    salesline.itemid as source_item_identifier,
    cast(null as varchar2) as org_unit_code,
    cast(null as varchar2) as invoice_document_company,
    cast(null as varchar2) as invoice_document_number,
    cast(null as varchar2) as invoice_document_type,
    cast(null as varchar2) as source_location_code,
    cast(null as varchar2) as source_lot_code,
    cast(null as varchar2) as lot_status_code,
    salesline.createddatetime as ordered_date,
    -- Using the last despatched date for a given order if the line has been
    -- cancelled.  Cancelled lines on fulfilled orders should be included in the
    -- service level (case fill) calculation and the despatched date is needed.
    case
        when salesline.salesstatus = 4
        then
            coalesce(
                despatched_date_max.deliverydate,
                invoiced_date_max.invoicedate,
                custpackingsliptrans.deliverydate,
                custinvoicetrans.invoicedate
            )
        else coalesce(custpackingsliptrans.deliverydate, custinvoicetrans.invoicedate)
    end as line_actual_ship_date,
    salestable.receiptdaterequested as line_sch_pick_up_date,
    cast(null as varchar2) as line_prom_ship_date,
    case
        when salesline.salesstatus = 4 then salesline.modifieddatetime else null
    end as line_cancelled_date,
    custinvoicetrans.invoicedate as line_invoice_date,
    salestable.receiptdaterequested as line_requested_date,
    cast(null as varchar2) as line_original_promised_date,
    cast(null as varchar2) as line_promised_delivery_date,
    coalesce(
        custpackingsliptrans.deliverydate, custinvoicetrans.invoicedate
    ) as line_gl_date,
    salesline.shippingdaterequested as required_delivery_date,
    iff(wbxcusttableext.leadtime = '', null, wbxcusttableext.leadtime) as lead_time,
    salesline.customerref as customer_po_number,
    cast(null as varchar2) as kit_source_item_identifier,
    cast(null as varchar2) as kit_line_number,
    cast(null as varchar2) as kit_component_number,
    cast(null as varchar2) as component_line_no,
    cast(null as varchar2) as price_override_flag,
    cast(null as varchar2) as cost_override_flag,
    salestable.payment as source_payment_terms_code,
    salesline.dlvterm as source_freight_handling_code,
    (
        case
            when salesline.salesunit is null or length(salesline.salesunit) = 0
            then 'CASE'
            else upper(salesline.salesunit)
        end
    ) as transaction_quantity_uom,
    (
        case
            when salesline.salesunit is null or length(salesline.salesunit) = 0
            then 'CASE'
            else upper(salesline.salesunit)
        end
    ) as transaction_price_uom,
    -- SALESLINE.SALESQTY AS SALES_TRAN_QUANTITY,
    (
        case
            when
                row_number() over (
                    partition by
                        salesline.dataareaid,
                        salesline.salestype,
                        salesline.salesid,
                        salesline.linenum,
                        salesline.itemid
                    order by
                        salesline.dataareaid,
                        salesline.salestype,
                        salesline.salesid,
                        salesline.linenum,
                        salesline.itemid
                )
                = 1
            then salesline.salesqty_line
            else 0
        end
    ) as sales_tran_quantity,
    -- SALESLINE.QTYORDERED AS ORDERED_TRAN_QUANTITY,
    (
        case
            when
                row_number() over (
                    partition by
                        salesline.dataareaid,
                        salesline.salestype,
                        salesline.salesid,
                        salesline.linenum,
                        salesline.itemid
                    order by
                        salesline.dataareaid,
                        salesline.salestype,
                        salesline.salesid,
                        salesline.linenum,
                        salesline.itemid
                )
                = 1
            then salesline.qtyordered_line
            else 0
        end
    ) as ordered_tran_quantity,
    -- COALESCE(CUSTPACKINGSLIPTRANS.QTY, CUSTINVOICETRANS.QTY) AS
    -- SHIPPED_TRAN_QUANTITY,
    case
        when custpackingsliptrans.qty is not null
        then
            (
                case
                    when
                        row_number() over (
                            partition by
                                custpackingsliptrans.origsalesid,
                                custpackingsliptrans.inventtransid,
                                custpackingsliptrans.deliverydate
                            order by
                                custpackingsliptrans.origsalesid,
                                custpackingsliptrans.inventtransid,
                                custpackingsliptrans.deliverydate
                        )
                        = 1
                    then custpackingsliptrans.qty
                    else 0
                end
            )
        when custinvoicetrans.qty is not null
        then
            (
                case
                    when
                        row_number() over (
                            partition by
                                custinvoicetrans.origsalesid,
                                custinvoicetrans.inventtransid,
                                custinvoicetrans.invoicedate
                            order by
                                custinvoicetrans.origsalesid,
                                custinvoicetrans.inventtransid,
                                custinvoicetrans.invoicedate
                        )
                        = 1
                    then custinvoicetrans.qty
                    else 0
                end
            )
        else 0
    end as shipped_tran_quantity,
    -- CASE WHEN SALESLINE.SALESSTATUS=4 THEN SALESLINE.QTYORDERED ELSE 0 END AS
    -- CANCEL_TRAN_QUANTITY,
    case
        when salesline.salesstatus = 4
        then
            (
                case
                    when
                        row_number() over (
                            partition by
                                salesline.dataareaid,
                                salesline.salestype,
                                salesline.salesid,
                                salesline.linenum,
                                salesline.itemid
                            order by
                                salesline.dataareaid,
                                salesline.salestype,
                                salesline.salesid,
                                salesline.linenum,
                                salesline.itemid
                        )
                        = 1
                    then salesline.qtyordered_line
                    else 0
                end
            )
        else 0
    end as cancel_tran_quantity,
    -- (CASE WHEN SALESLINE.SALESSTATUS IN (2, 3) THEN SALESLINE.QTYORDERED ELSE 0
    -- END) - ZEROIFNULL(COALESCE(CUSTPACKINGSLIPTRANS.QTY, CUSTINVOICETRANS.QTY)) AS
    -- SHORT_TRAN_QUANTITY,
    -- Only calc the Short Qty for the row where the Order Tran Qty is non-zero
    case
        when
            salesline.salesstatus in (2, 3)
            and (
                case
                    when
                        row_number() over (
                            partition by
                                salesline.dataareaid,
                                salesline.salestype,
                                salesline.salesid,
                                salesline.linenum,
                                salesline.itemid
                            order by
                                salesline.dataareaid,
                                salesline.salestype,
                                salesline.salesid,
                                salesline.linenum,
                                salesline.itemid
                        )
                        = 1
                    then salesline.salesqty_line
                    else 0
                end
            )
            <> 0
        then
            salesline.qtyordered_line
            - coalesce(custpackingsliptrans.qty_sum, custinvoicetrans.qty_sum, 0)
        else 0
    end as short_tran_quantity,
    cast(0 as number(19, 2)) as backord_tran_quantity,
    salesline.currencycode as transaction_currency,
    salesline.salesprice as trans_unit_tran_price,
    salesline.salesprice as trans_list_tran_price,
    cast(0 as number(21, 4)) as trans_extend_tran_price,
    salesline.costprice as tran_unit_tran_cost,
    cast(0 as number(21, 4)) as trans_extend_tran_cost,
    salesline.linedisc as trans_deduction_01_amt,
    cast(0 as number(38, 10)) as trans_deduction_02_amt,
    cast(0 as number(38, 10)) as trans_deduction_03_amt,
    cast(0 as number(38, 10)) as source_foreign_conv_rt,
    salesline.modifieddatetime as source_updated_datetime,
    cast(null as varchar2) as short_reason_code,
    cast(0 as number(38, 10)) as trans_conv_rt,
    -- , SALESLINE.LINEAMOUNT   AS TRANS_RPT_GRS_AMT
    (
        case
            when
                row_number() over (
                    partition by
                        salesline.dataareaid,
                        salesline.salestype,
                        salesline.salesid,
                        salesline.linenum,
                        salesline.itemid
                    order by
                        salesline.dataareaid,
                        salesline.salestype,
                        salesline.salesid,
                        salesline.linenum,
                        salesline.itemid
                )
                = 1
            then salesline.lineamount_line
            else 0
        end
    ) as trans_rpt_grs_amt,
    salesline.salesprice as trans_rpt_grs_price,
    cast(0 as number(38, 10)) as trans_rpt_net_amt,
    cast(0 as number(38, 10)) as trans_rpt_net_price,
    salesline.salesstatus as line_status_code,
    case
        when salesline.salesstatus = 1
        then
            (
                case
                    when
                        row_number() over (
                            partition by
                                salesline.dataareaid,
                                salesline.salestype,
                                salesline.salesid,
                                salesline.linenum,
                                salesline.itemid
                            order by
                                salesline.dataareaid,
                                salesline.salestype,
                                salesline.salesid,
                                salesline.linenum,
                                salesline.itemid
                        )
                        = 1
                    then
                        salesline.qtyordered_line - nvl(custpackingsliptrans.qty_sum, 0)
                    else 0
                end
            )
        else 0
    end as open_tran_quantity,
    salestable.paymmode as source_payment_instr_code,
    null as packingslipid,
    case
        when substr(salesline.salesid, 1, 2) in ('WE', 'IE') then '1' else '0'
    end as edi_indicator,  -- HJ 04 NOV 20,added 'EI' as well to be 1 
    trim(salestable.wbxcancelreasoncode) as cancel_reason_code,
    trim(wbxsocancelreasontable.reasoncomments) as cancel_reason_desc,
    custconfirmtrans.qty as trans_quantity_confirmed,
    custconfirmtrans.salesprice as trans_salesprice_confirmed,
    custconfirmtrans.lineamount as trans_lineamount_confirmed,
    custconfirmtrans.confirmdate as trans_confirmdate_confirmed,
    custconfirmtrans.salesunit as trans_uom_confirmed,
    custconfirmtrans.currencycode as trans_currency_confirmed,
    custinvoicetrans.source_object_id,
    cc.displayvalue as cost_centre,
    (
        case
            when
                row_number() over (
                    partition by
                        custinvoicetrans.origsalesid,
                        custinvoicetrans.inventtransid,
                        custinvoicetrans.invoicedate
                    order by
                        custinvoicetrans.origsalesid,
                        custinvoicetrans.inventtransid,
                        custinvoicetrans.invoicedate
                )
                = 1
            then custinvoicetrans.trans_invoice_grs_amt
            when
                row_number() over (
                    partition by
                        custpackingsliptrans.origsalesid,
                        custpackingsliptrans.inventtransid,
                        custpackingsliptrans.deliverydate
                    order by
                        custpackingsliptrans.origsalesid,
                        custpackingsliptrans.inventtransid,
                        custpackingsliptrans.deliverydate
                )
                = 1
            then custpackingsliptrans.amt_sum
            else 0
        end
    ) trans_invoice_grs_amt,
    (
        case
            when
                row_number() over (
                    partition by
                        custinvoicetrans.origsalesid,
                        custinvoicetrans.inventtransid,
                        custinvoicetrans.invoicedate
                    order by
                        custinvoicetrans.origsalesid,
                        custinvoicetrans.inventtransid,
                        custinvoicetrans.invoicedate
                )
                = 1
            then custinvoicetrans.trans_invoice_net_amt
            else 0
        end
    ) trans_invoice_net_amt,
    (
        case
            when
                row_number() over (
                    partition by
                        custinvoicetrans.origsalesid,
                        custinvoicetrans.inventtransid,
                        custinvoicetrans.invoicedate
                    order by
                        custinvoicetrans.origsalesid,
                        custinvoicetrans.inventtransid,
                        custinvoicetrans.invoicedate
                )
                = 1
            then custinvoicetrans.trans_invoice_disc_amt
            else 0
        end
    ) trans_invoice_disc_amt,
    (
        case
            when
                row_number() over (
                    partition by
                        custinvoicetrans.origsalesid,
                        custinvoicetrans.inventtransid,
                        custinvoicetrans.invoicedate
                    order by
                        custinvoicetrans.origsalesid,
                        custinvoicetrans.inventtransid,
                        custinvoicetrans.invoicedate
                )
                = 1
            then custinvoicetrans.base_invoice_grs_amt
            when
                row_number() over (
                    partition by
                        custpackingsliptrans.origsalesid,
                        custpackingsliptrans.inventtransid,
                        custpackingsliptrans.deliverydate
                    order by
                        custpackingsliptrans.origsalesid,
                        custpackingsliptrans.inventtransid,
                        custpackingsliptrans.deliverydate
                )
                = 1
            then custpackingsliptrans.amt_sum
            else 0
        end
    ) base_invoice_grs_amt,
    (
        case
            when
                row_number() over (
                    partition by
                        custinvoicetrans.origsalesid,
                        custinvoicetrans.inventtransid,
                        custinvoicetrans.invoicedate
                    order by
                        custinvoicetrans.origsalesid,
                        custinvoicetrans.inventtransid,
                        custinvoicetrans.invoicedate
                )
                = 1
            then custinvoicetrans.base_invoice_net_amt
            else 0
        end
    ) base_invoice_net_amt,
    (
        case
            when
                row_number() over (
                    partition by
                        custinvoicetrans.origsalesid,
                        custinvoicetrans.inventtransid,
                        custinvoicetrans.invoicedate
                    order by
                        custinvoicetrans.origsalesid,
                        custinvoicetrans.inventtransid,
                        custinvoicetrans.invoicedate
                )
                = 1
            then custinvoicetrans.base_invoice_disc_amt
            else 0
        end
    ) base_invoice_disc_amt,
    custinvoicetrans.variant_code as variant_code,
    salestable.wbxdeliveryinstruction as delivery_instruction,
    salestable.purchorderformnum as cust_order_number,
    salesline.wbxshelflife as item_shelf_life,
    wmspickingroute.pickingrouteid as picking_route,
    custconfirmtrans.dlvdate as trans_line_requested_date
from salesline
inner join salestable
    on salesline.salesid = salestable.salesid
    and upper(salesline.dataareaid) = upper(salestable.dataareaid) 
left join lkp_custinvoicejour as custinvoicejour
    on salestable.salesid = custinvoicejour.salesid
    and salestable.partition = custinvoicejour.partition
    and upper(salestable.dataareaid) = upper(custinvoicejour.dataareaid) 
left join lkp_custpackingsliptrans as custpackingsliptrans
    on salesline.salesid = custpackingsliptrans.origsalesid
    and salesline.inventtransid = custpackingsliptrans.inventtransid
    and upper(salestable.dataareaid) = upper(custpackingsliptrans.dataareaid) 
left join lkp_custinvoicetrans as custinvoicetrans
    on salesline.salesid = custinvoicetrans.origsalesid
    and salesline.inventtransid = custinvoicetrans.inventtransid
    and upper(salestable.dataareaid) = upper(custinvoicetrans.dataareaid)   
left join lkp_custconfirmtrans as custconfirmtrans
    on custconfirmtrans.salesid = salesline.salesid
    and custconfirmtrans.inventtransid = salesline.inventtransid
    and upper(salestable.dataareaid) = upper(custconfirmtrans.dataareaid) 
left join wbxcusttableext
    on salesline.custaccount = wbxcusttableext.custaccount
    and upper(salestable.dataareaid) = upper(wbxcusttableext.dataareaid) 
left join wbxsocancelreasontable
    on salestable.wbxcancelreasoncode = wbxsocancelreasontable.reasoncode
    and upper(salestable.dataareaid) = upper(wbxsocancelreasontable.dataareaid) 
left join lkp_dimensionattributevaluesetitem cc
    on salesline.partition = cc.partition
    and salesline.defaultdimension = cc.dimensionattributevalueset
left join despatched_date_max
    on salesline.salesid = despatched_date_max.origsalesid
    and upper(salestable.dataareaid) = upper(despatched_date_max.dataareaid) 
left join invoiced_date_max
    on salesline.salesid = invoiced_date_max.origsalesid
    and upper(salestable.dataareaid) = upper(invoiced_date_max.dataareaid) 
left join lkp_wmspickingroute wmspickingroute
    on salesline.salesid = wmspickingroute.transrefid
    and upper(salestable.dataareaid) = upper(wmspickingroute.dataareaid) 
