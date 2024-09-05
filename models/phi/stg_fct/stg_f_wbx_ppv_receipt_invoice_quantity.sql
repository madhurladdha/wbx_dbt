/*

this is model is equivalent to the view POSTSNOWP.R_EI_SYSADM.V_PPV_WTX_RECEIPT_INVOICE_QUANTITY
which is being used in the map m_PRC_WTX_PPV_FACT

After doing minus from dbt and view 0 records and vice versa

*/


with
    cte_inventtrans as (
        select
            itemid itemid_trans,
            voucherphysical as voucher,
            dataareaid,
            partition,
            currencycode,
            sum(qty) as receipt_quantity
        from {{ ref("src_inventtrans") }}
        group by 1, 2, 3, 4, 5
    ),
    cte_voucher as (
        select
            min(ito.referenceid) as po_order_number,
            itp.voucher as voucher,
            max(pt.currencycode) as currencycode,
            itp.itemid,
            iigi.itemgroupid,
            itp.partition,
            itp.dataareaid,
            max(transbegintime) as transbegintime
        from {{ ref("src_inventtransposting") }} itp  -- weetabix.inventtransposting 
        inner join
            {{ ref("src_inventtransorigin") }} ito
            on itp.partition = ito.partition
            and itp.dataareaid = ito.dataareaid
            and itp.inventtransorigin = ito.recid
        inner join
            {{ ref("src_inventitemgroupitem") }} iigi
            on upper(trim(itp.dataareaid)) = upper(trim(iigi.itemdataareaid))
            and to_char(itp.itemid) = to_char(iigi.itemid)
        inner join
            {{ ref("src_purchtable") }} pt
            on pt.purchid = ito.referenceid
            and upper(pt.dataareaid) = upper(itp.dataareaid)
        inner join
            {{ ref("src_purchline") }} pl
            on upper(trim(pt.dataareaid)) = upper(trim(pl.dataareaid))
            and pt.purchid = pl.purchid
        inner join
            {{ ref("src_inventdim") }} id
            on upper(trim(pt.dataareaid)) = upper(trim(pl.dataareaid))
            and pl.inventdimid = id.inventdimid
        inner join
            {{ ref("src_purchparmtable") }} ppt
            on pt.purchid = ppt.purchid
            and upper(pt.dataareaid) = upper(ppt.dataareaid)
        inner join
            {{ ref("src_purchparmline") }} ppl
            on ppt.tablerefid = ppl.tablerefid
            and ppt.parmid = ppl.parmid
            and ppt.enddatetime <> '1900-01-01'
        where
            ito.referenceid <> ''
            and transbegintime >= '2019-10-01'
            and upper(iigi.itemgroupid)
            in ('WHEAT', 'RAWMATS', 'PACKAGING', 'STRETCH', 'NONBOM', '3RDPARTY')
        group by
            itp.partition, itp.dataareaid, itp.voucher, itp.itemid, iigi.itemgroupid
        having count(distinct ito.referenceid) < 2
    ),
    cte_vendtrans as (
        select
            origpurchid as po_order_number,
            v.voucher as voucher,
            v.dataareaid,
            v.partition,
            v.transdate,
            vit.itemid itemid_vit,
            vit.qty invoiced_qty,
            salesbalance invoiced_amount
        from {{ ref("src_vendtrans") }} v
        inner join
            {{ ref("src_vendinvoicetrans") }} vit
            on v.invoice = vit.invoiceid
            and upper(trim(v.dataareaid)) = upper(trim(vit.dataareaid))
        inner join
            {{ ref("src_vendinvoicejour") }} vij
            on v.invoice = vij.invoiceid
            and v.voucher = vij.ledgervoucher
            and upper(trim(v.dataareaid)) = upper(trim(vij.dataareaid))
    ),
    cte_int as (
        select
            a.po_order_number,
            a.voucher as voucher,
            upper(a.dataareaid) po_order_company,
            itemid as source_item_identifier,
            nvl(c.transdate, a.transbegintime) as po_received_date,
            a.currencycode as transaction_currency,
            nvl(b.receipt_quantity, 0) receipt_quantity,
            nvl(c.invoiced_qty, 0) invoiced_qty,
            nvl(c.invoiced_amount, 0) invoiced_amount
        from cte_voucher a
        left join
            cte_inventtrans b
            on a.voucher = b.voucher
            and upper(a.dataareaid) = upper(b.dataareaid)
            and a.partition = b.partition
            and a.itemid = b.itemid_trans
        left join
            cte_vendtrans c
            on a.po_order_number = c.po_order_number
            and upper(a.dataareaid) = upper(c.dataareaid)
            and a.partition = c.partition
            and a.voucher = c.voucher
            and a.itemid = c.itemid_vit
    ),
    cte_final as 
    (select po_order_number,
voucher,
po_order_company,
source_item_identifier,
to_date(po_received_date) as po_received_date,
transaction_currency,
sum(receipt_quantity) as receipt_quantity,
sum(invoiced_qty) as invoiced_qty,
sum(invoiced_amount) as invoiced_amount from cte_int
group by po_order_number,
voucher,
po_order_company,
source_item_identifier,
po_received_date,
transaction_currency
)
select * from cte_final
