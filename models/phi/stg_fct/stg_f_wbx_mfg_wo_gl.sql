{{
    config(
        tags = ["wbx","manufacturing","work order","gl","yield"],
        snowflake_warehouse=env_var("DBT_WBX_SF_WH")
    )
}}

with inventtransposting as (
    select * from {{ ref('src_inventtransposting') }}
),

inventtransorigin as (
    select * from {{ ref('src_inventtransorigin') }}
),

dimensionattributelevelvalueview as (
    select * from {{ ref('src_dimensionattributelevelvalueview') }}
),

subledgervouchergeneraljournal as (
    select * from {{ ref('src_subledgervouchergeneraljournal') }}
),

dimensionattribute as (
    select * from {{ ref('src_dimensionattribute') }}
),

ledger as (
    select * from {{ ref('src_ledger') }}
),

vouchref as (
    select
        itp.partition,
        itp.dataareaid,
        itp.voucher,
        min(ito.referenceid) as referenceid,
        max(transbegintime) as transbegintime
    from inventtransposting itp
    inner join
        inventtransorigin ito
        on itp.partition = ito.partition
        and itp.dataareaid = ito.dataareaid
        and itp.inventtransorigin = ito.recid
    where ito.referenceid <> ''
    -- AND TRANSDATE BETWEEN '2019-04-01' AND '2019-04-30'
    group by itp.partition, itp.dataareaid, itp.voucher
    having count(distinct ito.referenceid) < 2
),

dm0 as (
    select dalvv.displayvalue, dalvv.valuecombinationrecid, da.name, dalvv.partition
    from dimensionattributelevelvalueview dalvv
    inner join dimensionattribute  da on dalvv.dimensionattribute = da.recid
    where da.name = 'MainAccount'
),

dm1 as (
    select dalvv.displayvalue, dalvv.valuecombinationrecid, da.name, dalvv.partition
    from dimensionattributelevelvalueview dalvv
    inner join dimensionattribute  da on dalvv.dimensionattribute = da.recid
    where da.name = 'Sites'
),

dm2 as (
    select dalvv.displayvalue, dalvv.valuecombinationrecid, da.name, dalvv.partition
    from dimensionattributelevelvalueview dalvv
    inner join dimensionattribute  da on dalvv.dimensionattribute = da.recid
    where da.name = 'CostCenters'
),

dm3 as (
    select dalvv.displayvalue, dalvv.valuecombinationrecid, da.name, dalvv.partition
    from dimensionattributelevelvalueview dalvv
    inner join dimensionattribute  da on dalvv.dimensionattribute = da.recid
    where da.name = 'Purpose'
),

dm4 as (
    select dalvv.displayvalue, dalvv.valuecombinationrecid, da.name, dalvv.partition
    from dimensionattributelevelvalueview dalvv
    inner join dimensionattribute  da on dalvv.dimensionattribute = da.recid
    where da.name = 'Plant'
),

dm5 as (
    select dalvv.displayvalue, dalvv.valuecombinationrecid, da.name, dalvv.partition
    from dimensionattributelevelvalueview dalvv
    inner join dimensionattribute  da on dalvv.dimensionattribute = da.recid
    where da.name = 'Customer'
),

dm6 as (
    select dalvv.displayvalue, dalvv.valuecombinationrecid, da.name, dalvv.partition
    from dimensionattributelevelvalueview dalvv
    inner join dimensionattribute  da on dalvv.dimensionattribute = da.recid
    where da.name = 'ProductClass'
),

generaljournalaccountentry as (
    select * from {{ ref('src_generaljournalaccountentry') }}
),

generaljournalentry as (
    select * from {{ ref('src_generaljournalentry') }}
    
),

stage_table as (
    select
        '{{env_var("DBT_SOURCE_SYSTEM")}}' as source_system,
        l.name as document_company,
        case
            gje.journalcategory
            when 0
            then 'None'
            when 1
            then 'Transfer'
            when 2
            then 'Sales Order'
            when 3
            then 'Purchase Order'
            when 4
            then 'Stock'
            when 5
            then 'Production'
            when 6
            then 'Project'
            when 7
            then 'Interest'
            when 8
            then 'Customer'
            when 9
            then 'Foreign Currency Revaluation'
            when 10
            then 'Totalled'
            when 11
            then 'Payroll'
            when 12
            then 'Fixed assets'
            when 13
            then 'Dunning Letter'
            when 14
            then 'Supplier'
            when 15
            then 'Payment'
            when 16
            then 'VAT'
            when 17
            then 'BANK'
            when 18
            then 'Ledger Accounting Currency Conversion'
            when 19
            then 'Bill of Exchange'
            when 20
            then 'Promissory note'
            when 21
            then 'Cost'
            when 22
            then 'Labour'
            when 23
            then 'Fee'
            when 24
            then 'Settlement'
            when 25
            then 'Allocation'
            when 26
            then 'Elimination'
            when 27
            then 'Settlement Discount'
            when 28
            then 'Overpayment/underpayment'
            when 29
            then 'Penny Difference'
            when 30
            then 'Intercompany Settlement'
            when 31
            then 'Purchase Requisition'
            when 32
            then 'Inflation Adjustment'
            when 33
            then 'Prepayment application'
            when 34
            then 'Ledger reporting currency conversion'
            when 80
            then 'AR Amortization'
            when 81
            then 'Deferrals'
            when 82
            then 'AP Amortization'
            when 83
            then 'Advance Adjustment'
            when 84
            then 'Tax agent'
            when 85
            then 'Currency conversion gain/loss'
            when 100
            then 'Rebate credit note processing'
            when 101
            then 'Rebate pass to AP'
            when 35
            then 'Write off'
            when 36
            then 'General Journal'
            when 251
            then 'Underpayment write off'
            else 'Unknown'
        end as document_type,
        to_char(gjae.recid) as document_number,
        sl.voucher as voucher,
        gje.journalnumber as journal_number,
        nvl(vouchref.referenceid, '') as reference_id,
        gje.accountingdate as gl_date,
        nvl(dm1.displayvalue, '') as source_site_code,
        upper(nvl(dm4.displayvalue, '')) as source_business_unit_code,
        nvl(dm2.displayvalue, '') as cost_center_code,
        gje.createddatetime as source_date_updated,
        nvl(dm6.displayvalue, '') as product_class,
        dm0.displayvalue as source_account_identifier,
        gjae.transactioncurrencycode as transaction_currency,
        gjae.reportingcurrencyamount as transaction_amount,
        gjae.text as remark_txt,
        vouchref.transbegintime as recipecalc_date
    from generaljournalaccountentry gjae
    inner join
        dm0
        on gjae.partition = dm0.partition
        and gjae.ledgerdimension = dm0.valuecombinationrecid
    inner join
        generaljournalentry gje
        on gjae.partition = gje.partition
        and gjae.generaljournalentry = gje.recid
    inner join ledger l on gje.partition = l.partition and gje.ledger = l.recid
    inner join
        subledgervouchergeneraljournal sl
        on gje.recid = sl.generaljournalentry
        and gje.partition = sl.partition
    left outer join
        dm1
        on gjae.partition = dm1.partition
        and gjae.ledgerdimension = dm1.valuecombinationrecid
    left outer join
        dm2
        on gjae.partition = dm2.partition
        and gjae.ledgerdimension = dm2.valuecombinationrecid
    left outer join
        dm4
        on gjae.partition = dm4.partition
        and gjae.ledgerdimension = dm4.valuecombinationrecid
    left outer join
        dm6
        on gjae.partition = dm6.partition
        and gjae.ledgerdimension = dm6.valuecombinationrecid
    left outer join
        vouchref
        on sl.partition = vouchref.partition
        and upper(sl.voucherdataareaid) = upper(vouchref.dataareaid)
        and vouchref.voucher = sl.voucher
    where
        dm0.displayvalue in (
            '550010',
            '550015',
            '550030',
            '510045',
            '718020',
            '718040',
            '718060',
            '540010',
            '540015',
            '540005'
        )
        and gje.postinglayer = 0
        and left(sl.voucher, 2) <> 'YE'
        and left(sl.voucher, 7) <> 'Closing'
),

final as (
    select 
        *,
        current_timestamp() as load_date,
        current_timestamp() as update_date       
    from stage_table 
)

select * from final
