{{ config(tags=["finance", "po"]) }}

{% set now = modules.datetime.datetime.now() %}
{%- set full_load_day -%} {{env_var('DBT_FULL_LOAD_DAY')}} {%- endset -%}
{%- set day_today -%} {{ now.strftime('%A') }} {%- endset -%}

with
    purchline as (
        select *
        from {{ ref("src_purchline") }}

        /*  Removing incremental logic filter until can be properly fixed.  Believe that the PO might be correct, but PO Receipt is not working so removing
            from both for now.
    */
    ),

    purchtable as (select * from {{ ref("src_purchtable") }}),

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

    dimensionjoincaf as (
        select dalvv.displayvalue, dalvv.valuecombinationrecid, da.name, dalvv.partition
        from dimensionattributelevelvalueview dalvv
        inner join dimensionattribute da on dalvv.dimensionattribute = da.recid
        where da.name = 'Purpose'
    ),

    accountingdistributionjoin as (
        select
            nvl(cc.displayvalue, '-') as cost_center,
            nvl(caf.displayvalue, '-') as caf_no,
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
        left outer join
            dimensionjoincaf caf on ad.ledgerdimension = caf.valuecombinationrecid
        where ad.number_ = 1
    ),

    agreementheader as (select * from {{ ref("src_agreementheader") }}),

    vendpackingsliptrans as (
        select inventtransid, max(deliverydate) as deliverydate
        from {{ ref("src_vendpackingsliptrans") }}
        group by inventtransid
    ),

    projtable as (select * from {{ ref("src_projtable") }})


select
    '{{ env_var("DBT_SOURCE_SYSTEM") }}' as source_system,
    cast(upper(trim(pt.dataareaid)) as varchar2(255)) as po_order_company,
    cast(pt.purchid as varchar2(255)) as po_order_number,
    cast(cast(pt.purchasetype as integer) as string(255)) as po_order_type,
    cast('-' as varchar2(255)) as po_order_suffix,
    cast(pl.linenumber as number(38,10)) as po_line_number,
    cast(cast(pl.purchstatus as integer) as string(255)) as line_status,
    cast(pl.purchasetype as varchar2(255)) as line_type,
    cast(substr(pl.name, 1, 255) as varchar2(255)) as po_line_desc,
    case
        when id.inventlocationid = ''
        then ' '
        else cast(upper(trim(id.inventlocationid)) as varchar2(255))
    end as source_business_unit_code,
    cast(pt.invoiceaccount as varchar2(255)) as source_supplier_identifier,
    case
        when trim(pt.payment) = '' or trim(pt.payment) is null
        then '-'
        else cast(trim(pt.payment) as varchar2(255))
    end as source_payment_terms_code,
    cast(to_char(pt.dlvterm) as varchar2(255)) as source_freight_handling_code,
    cast(null as string(255)) as source_buyer_identifier,
    cast(ltrim(rtrim(adj.mainaccount)) as varchar2(255)) as source_account_identifier,
    case
        when pl.itemid = '' then ' ' else cast(pl.itemid as varchar2(255))
    end as source_item_identifier,
    cast(null as string(255)) as source_subledger_identifier,
    cast(null as string(255)) as source_subledger_type,
    cast(null as string(255)) as contract_company_code,
    cast(null as string(255)) as contract_number,
    cast(null as string(255)) as contract_type,
    cast(null as string(255)) as contract_line_number,
    cast(null as string(255)) as agrmnt_number,
    cast(null as string(255)) as agrmnt_suplmnt_number,
    cast(null as string(255)) as gl_offset_srccd,
    pt.accountingdate as po_gl_date,
    pt.createddatetime as po_order_date,
    vt.deliverydate as po_delivery_date,
    pl.confirmeddlv as po_promised_delivery_date,
    pl.deliverydate as po_requested_date,
    cast(
        to_char(
            case when pl.purchstatus = 4 then pl.modifieddatetime else null end
        ) as varchar2(255)
    ) as po_cancelled_date,
    cast(
        to_char(
            case
                when pl.priceunit <> 0
                then
                    cast(pl.purchprice as number(38, 10))
                    / cast(pl.priceunit as number(38, 10))
                else pl.purchprice
            end
        ) as varchar2(255)
    ) as line_unit_cost,
    0 as line_on_hold_amt,
    cast(
        to_char(
            cast(pl.remainpurchphysical as number(38, 10)) * (
                case
                    when pl.priceunit <> 0
                    then
                        cast(pl.purchprice as number(38, 10))
                        / cast(pl.priceunit as number(38, 10))
                    else pl.purchprice
                end
            )
        ) as varchar2(255)
    ) as line_open_amt,
    cast(
        to_char(
            cast((pl.purchqty - pl.remainpurchphysical) as number(38, 10)) * (
                case
                    when pl.priceunit <> 0
                    then
                        cast(pl.purchprice as number(38, 10))
                        / cast(pl.priceunit as number(38, 10))
                    else pl.purchprice
                end
            )
        ) as varchar2(255)
    ) as line_received_amt,
    0 as line_onhold_quantity,
    cast(to_char(pl.remainpurchphysical) as varchar2(255)) as line_open_quantity,
    cast(to_char(pl.purchqty) as varchar2(255)) as line_order_quantity,
    cast(
        to_char(pl.purchqty - pl.remainpurchphysical) as varchar2(255)
    ) as line_recvd_quantity,
    cast(pl.purchunit as varchar2(255)) as transaction_uom,
    cast(pl.currencycode as varchar2(255)) as transaction_currency,
    cast(null as string(255)) as contract_agreement_flag,
    cast(pl.vendgroup as varchar2(255)) as source_contract_type,
    cast(null as string(255)) as po_original_dlv_date,
    trim(pt.returnitemnum) as rma_number,
    nvl(trim(pa.purchnumbersequence), '-') as agreement_number,
    pl.modifieddatetime as source_updated_datetime,
    adj.cost_center as source_cost_center,
    adj.caf_no as caf_no,
    pt.projid as project_id,
    proj.name as project_name,
    pl.projcategoryid as project_category,
    adj.mainaccountid as source_object_code,
    lineamount as line_total_amount
from purchtable pt
inner join
    purchline pl
    on upper(trim(pt.dataareaid)) = upper(trim(pl.dataareaid))
    and pt.purchid = pl.purchid
inner join
    inventdim id
    on upper(trim(pt.dataareaid)) = upper(trim(pl.dataareaid))
    and pl.inventdimid = id.inventdimid
    -- condition added for 'IRE' on 18 Nov 2020
    and upper(trim(id.dataareaid)) = upper(trim(pt.dataareaid))
-- --This sub-select captures the SOURCE_ACCOUNT_IDENTIFER and SOURCE_COST_CENTER
-- which are required for proper lookup on FIN_ACCOUNT_DIM
-- --Adjusted the join for the Cost Center to be an outer join as not all accounts
-- that need to be assigned have a real Cost Center.
left outer join
    accountingdistributionjoin adj
    on pt.sourcedocumentheader = adj.sourcedocumentheader
    and pl.sourcedocumentline = adj.sourcedocumentline
left outer join agreementheader pa on pt.matchingagreement = pa.recid
left outer join vendpackingsliptrans vt on pl.inventtransid = vt.inventtransid
left outer join projtable proj on pt.projid = proj.projid
