{{
    config(
        tags=["sales", "actuals","sales_actuals"]
    )
}}

with salestable as (
    select * from {{ ref('src_salestable') }}
),

custinvoicejour as (
    select 
        salesid, 
        partition, 
        dataareaid, 
        max(invoicedate) as invoicedate
    from {{ ref('src_custinvoicejour') }}
    group by 1, 2, 3
),

custpackingslipjour as (
    select 
        salesid, 
        dataareaid, 
        min(deliverydate) as deliverydate
    from {{ ref('src_custpackingslipjour') }}
    group by salesid, dataareaid
),

mcrholdcodetrans as (
    select
        mcrcleared,
        inventrefid,
        dataareaid,
        modifieddatetime
    from {{ ref('src_mcrholdcodetrans') }}
    qualify  row_number() over ( partition by inventrefid, dataareaid order by modifieddatetime desc ) = 1

),

final as (
    select
        'WEETABIX' as source_system,
        salestable.salesid as sales_order_number,
        salestable.salestype as source_sales_order_type,
        upper(trim(salestable.dataareaid)) as sales_order_company,
        salestable.inventlocationid as source_business_unit_code,
        salestable.custaccount as ship_source_customer_code,
        salestable.invoiceaccount as bill_source_customer_code,
        case
            when salestable.wbxsenddatetime = '01-JAN-1900'
            then salestable.createddatetime
            else salestable.wbxsenddatetime
        end as ordered_date,
        salestable.receiptdaterequested as sched_pick_date,
        case
            when salestable.salesstatus = 4 then salestable.modifieddatetime else null
        end as cancelled_date,
        custinvoicejour.invoicedate as invoice_date,
        salestable.fixedduedate as requested_date,
        custpackingslipjour.deliverydate as actual_ship_date,
        cast(null as varchar2(1)) as actl_ship_reason_code,
        cast(null as varchar2(1)) as actl_ship_reason_desc,
        salestable.deliverydate as arrival_date,
        cast(null as varchar2(1)) as arrival_reason_code,
        cast(null as varchar2(1)) as arrival_reason_desc,
        salestable.shippingdaterequested as revised_crad_date,
        salestable.shippingdaterequested as crad_date,
        mcrholdcodetrans.mcrcleared as hold_status,
        salestable.dlvmode as carr_trsp_mode_code,
        salestable.modifieddatetime as source_updated_datetime,
        salestable.salesstatus as header_status_code
    from salestable
    left join custinvoicejour
        on salestable.salesid = custinvoicejour.salesid
        and salestable.partition = custinvoicejour.partition
        and salestable.dataareaid = custinvoicejour.dataareaid 
    left join mcrholdcodetrans
        on salestable.salesid = mcrholdcodetrans.inventrefid
        and upper(salestable.dataareaid) = upper(mcrholdcodetrans.dataareaid)  
    left join custpackingslipjour  
        on salestable.salesid = custpackingslipjour.salesid
        and upper(salestable.dataareaid) = upper(custpackingslipjour.dataareaid)
)

select * from final
