{{ config(tags=["finance", "gl","gl_trans"],
snowflake_warehouse=env_var('DBT_WBX_SF_WH')) }}

{% set now = modules.datetime.datetime.now() %}
{%- set full_load_day -%} {{env_var('DBT_FULL_LOAD_DAY')}} {%- endset -%}
{%- set day_today -%} {{ now.strftime('%A') }} {%- endset -%}

with 
fintag as
(
    select * from {{ ref('src_fintag')}}
),
account_combination as
(
    select 
        combination.display_value account_display_value,main_account_value main_account,
        costcenters,costcenters.value cost_center,costcenters.description cost_center_name,
        productclass,productclass.value product_class,productclass.description product_class_name,
        usaccount,usaccount.value us_account,usaccount.description us_account_name 
    from  {{ ref('src_dimensionattributevaluecombination')}}  combination  
    left join {{ ref('src_dimensionfinancialtag')}} costcenters on costcenters.recid = combination.costcenters
    left join {{ ref('src_dimensionfinancialtag')}} productclass on productclass.recid = combination.productclass
    left join {{ ref('src_dimensionfinancialtag')}} usaccount on usaccount.recid = combination.usaccount
),
account_string as
(
    select   
        distinct account_display_value,main_account,cost_center, cost_center_name,product_class,product_class_name,
        us_account, us_account_name
    from account_combination
),
generaljournalaccountentry as (
    select * from {{ ref('src_generaljournalaccountentry')}}
),

generaljournalentry as (
    select 
        subledgervoucher,
        accountingdate,
        journalnumber,
        documentdate,
        journalcategory,
        createddatetime,
        recid,
        ledger,
        fiscalcalendarperiod,
        upper(trim(subledgervoucherdataareaid)) as subledgervoucherdataareaid,
        ledgerentryjournal    

     from {{ ref('src_generaljournalentry')}}
),

ledger as (
    select * from {{ ref('src_ledger')}}
),

mainaccount as (
    select * from {{ ref('src_mainaccount')}}
),

dimensionattributevaluecombo as (
    select * from {{ ref('src_dimensionattributevaluecombo')}}
),

dimensionattributelevelvalueview as (
    select * from {{ ref('src_dimensionattributelevelvalueview')}}
),

dimensionattribute as (
    select * from {{ ref('src_dimensionattribute')}}
),

mainaccountcategory as (
    select * from {{ ref('src_mainaccountcategory')}}
),

ledgerentry as (
    select * from {{ ref('src_ledgerentry')}}
),

ledgerentryjournal as (
    select * from {{ ref('src_ledgerentryjournal')}}
),

ledgerjournaltable as (
    select 
        journaltype,
        journalnum,
        upper(trim(dataareaid)) as dataareaid

    from {{ ref('src_ledgerjournaltable')}}
),

fiscalcalendarperiod as (
    select 
        recid,
        fiscalcalendaryear,
        type,
        startdate,
        enddate    
    from {{ ref('src_fiscalcalendarperiod')}}
    where type =1
),

fiscalcalendaryear as (
    select * from {{ ref('src_fiscalcalendaryear')}}
),

vtr as (
    select 
        distinct voucher, 
        upper(trim(dataareaid)) as dataareaid, 
        transdate 
    from {{ ref('src_vendtrans')}}
),

ctr as (
    select 
        distinct voucher, 
        upper(trim(dataareaid)) as dataareaid, 
        transdate 
    from {{ ref('src_custtrans')}}
),

davc2 as (
    select 
        displayvalue, 
        ledgerdimensiontype, 
        max(mainaccount) 
    from dimensionattributevaluecombo 
    where ledgerdimensiontype = '1'
    group by displayvalue, ledgerdimensiontype
),

sbu as (
    select 
        dalvv.displayvalue,
        dalvv.valuecombinationrecid,
        da.name,dalvv.partition 
    from dimensionattributelevelvalueview dalvv 
    inner join dimensionattribute da on dalvv.dimensionattribute = da.recid 
    where da.name = 'CostCenters'
),

dept as (
    select 
        dalvv.displayvalue,
        dalvv.valuecombinationrecid,
        da.name,dalvv.partition 
    from {{ ref('src_dimensionattributelevelvalueview')}} dalvv 
    inner join {{ ref('src_dimensionattribute')}} da    
    on dalvv.dimensionattribute = da.recid 
    where da.name = 'Department'
),

caf as (
    select 
        dalvv.displayvalue,
        dalvv.valuecombinationrecid,
        da.name,
        dalvv.partition 
    from {{ ref('src_dimensionattributelevelvalueview')}} dalvv 
    inner join {{ ref('src_dimensionattribute')}} da 
    on dalvv.dimensionattribute = da.recid 
    where da.name = 'Purpose'
),

plant as  (
      select 
          dalvv.displayvalue,
          dalvv.valuecombinationrecid,
          da.name,
          dalvv.partition 
       from {{ ref('src_dimensionattributelevelvalueview')}} dalvv 
       inner join {{ ref('src_dimensionattribute')}} da 
       on dalvv.dimensionattribute = da.recid 
       where da.name = 'Plant'
), 

prod_class as (           
           select 
               dalvv.displayvalue,
               dalvv.valuecombinationrecid,
               da.name,dalvv.partition 
            from {{ ref('src_dimensionattributelevelvalueview')}} dalvv 
            inner join {{ ref('src_dimensionattribute')}} da 
            on dalvv.dimensionattribute = da.recid 
            where da.name = 'ProductClass'
),

trade_type as (
           select 
               dalvv.displayvalue,
               dalvv.valuecombinationrecid,
               da.name,
               dalvv.partition 
            from {{ ref('src_dimensionattributelevelvalueview')}} dalvv 
            inner join {{ ref('src_dimensionattribute')}} da 
            on dalvv.dimensionattribute = da.recid 
            where da.name = 'Customer'
),

source as (
    select 
   '{{env_var("DBT_SOURCE_SYSTEM")}}'                                       as source_system, 
    l.name                                                                  as document_company
    ,case when vtr.voucher is null then 
    case when ctr.voucher is null then substr(gje.subledgervoucher, 1, 3)
    else 'C_'||substr(ctr.voucher, 1, 3) end else case 
    when ctr.voucher is null then 'V_'||substr(vtr.voucher, 1, 3) 
    else case when upper(trim(mac.accountcategory)) = 'CREDITORS' 
    then 'V_'||substr(vtr.voucher, 1, 3) 
    else 'C_'||substr(ctr.voucher, 1, 3) end end end                        as source_document_type
    , gjae.generaljournalentry                                              as document_number
    , ma.recid                                                              as source_account_identifier
    , gje.accountingdate                                                    as gl_date
    , to_number(gjae.recid)                                                 as journal_line_number 
    , coalesce(cast(davc2.ledgerdimensiontype as nvarchar2(2)), '-')        as gl_extension_code 
    , coalesce(trim(sbu.displayvalue), '-')                                 as source_business_unit_code
    , coalesce(to_char(gjae.iscorrection),'-')                              as void_flag
    , gje.accountingdate                                                    as payment_trans_date 
    , '-'                                                                   as document_pay_item
    , '-'                                                                   as payment_number 
    , '-'                                                                   as payment_identifier
    , l.accountingcurrency                                                  as base_currency
    , coalesce(to_char(davc2.ledgerdimensiontype),'0')                      as source_ledger_type
    , case when trim(lej.journalnumber) = '' then 'NO' 
    when trim(lej.journalnumber) is null then 'NO' else 'YES' end           as gl_posted_flag
    , coalesce(gje.journalnumber,'-')                                       as batch_number
    , '-'                                                                   as batch_type
    , gje.documentdate                                                      as batch_date
    , '-'                                                                   as source_address_number
    , coalesce(upper(trim(gje.subledgervoucherdataareaid)),'-')             as company_code
    , cast(gjae.transactioncurrencyamount as number(38,10))                 as txn_ledger_amt
    , coalesce(gjae.transactioncurrencycode,'-')                            as transaction_currency
    , coalesce(gjae.accountingcurrencyamount,0)                             as base_ledger_amt
    , cast(0 as number(38,10))                                              as txn_quantity
    , '-'                                                                   as transaction_uom
    , '-'                                                                   as supplier_invoice_number
    , cast(null as varchar(255))                                            as invoice_date
    , coalesce(mac.accountcategory,'-')                                     as account_cat21_code
    , case when trim(lej.journalnumber) = '' then '-' 
    when lej.journalnumber is null then '-' else 
    'Ledger Journal No:  ' || trim(lej.journalnumber) ||' Type:  '|| 
    case when trim(ljt.journaltype) = '' then '-' when trim(ljt.journaltype) 
    is null then '' else trim(ljt.journaltype) end end                      as explanation_txt,
    case when lej.journalnumber is not null then
    trim(lej.journalnumber)  end                                            as transledger_journal_number
    , coalesce(cast(gjae.text || ' Journal Cat:  ' || gje.journalcategory 
    as varchar(255)),'-')                                                   as remark_txt
    , coalesce(trim(gje.journalnumber), '-')                                as reference1_txt 
    , coalesce(trim(gje.subledgervoucher), '-')                             as reference2_txt 
    , '-'                                                                   as reference3_txT
    , gje.createddatetime                                                   as createddatetime
    ,dept.displayvalue                                                      as department
    ,caf.displayvalue                                                       as caf_no
    ,plant.displayvalue                                                     as plant
    ,prod_class.displayvalue                                                as product_dim
    ,trade_type.displayvalue                                                as customer_dim
    ,gje.journalcategory                                                    as journal_category 
    ,ma.mainaccountid                                                       as source_object_code
    ,gje.createddatetime                                                    as source_date_updated
    ,gjae.ledgeraccount                                                    as ledger_account
    ,tag.tag_01                                                             as trade_type
    ,tag.tag_02                                                             as sku
    ,tag.tag_03                                                             as Promo_Term_Code
    ,main_account                                                           as main_account
    ,ma.name                                                                as main_account_name 
    ,account_string.cost_center                                             as cost_center
    ,account_string.cost_center_name                                        as cost_center_name
    ,account_string.product_class                                           as product_class
    ,account_string.product_class_name                                      as product_class_name
    ,account_string.us_account                                              as us_account
    ,account_string.us_account_name                                         as us_account_name
from generaljournalaccountentry gjae
    inner join generaljournalentry gje on gje.recid = gjae.generaljournalentry
    inner join ledger l on gje.ledger = l.recid
    inner join mainaccount ma on ma.recid = gjae.mainaccount
    left outer join  davc2 on ma.mainaccountid = davc2.displayvalue
    inner join fiscalcalendarperiod fcp on fcp.recid = gje.fiscalcalendarperiod
    inner join fiscalcalendaryear fcy on fcp.fiscalcalendaryear = fcy.recid
    left outer join sbu on gjae.partition = sbu.partition 
    and gjae.ledgerdimension = sbu.valuecombinationrecid
    left outer join mainaccountcategory mac on ma.accountcategoryref = mac.accountcategoryref and ma.source = mac.source
    left outer join ledgerentry le on gjae.recid = le.generaljournalaccountentry
    left outer join ledgerentryjournal lej on gje.ledgerentryjournal = lej.recid
    left outer join ledgerjournaltable ljt 
    on lej.journalnumber = ljt.journalnum 
    and gje.subledgervoucherdataareaid = ljt.dataareaid
    left outer join vtr on gje.subledgervoucher = vtr.voucher and gje.accountingdate = vtr.transdate
    and gje.subledgervoucherdataareaid = vtr.dataareaid
    left outer join ctr on gje.subledgervoucher = ctr.voucher and gje.accountingdate = ctr.transdate
    and gje.subledgervoucherdataareaid = ctr.dataareaid
    left outer join dept on gjae.partition = dept.partition 
    and gjae.ledgerdimension = dept.valuecombinationrecid
    left outer join caf on gjae.partition = caf.partition 
    and gjae.ledgerdimension = caf.valuecombinationrecid
    left outer join plant on gjae.partition = plant.partition 
    and gjae.ledgerdimension = plant.valuecombinationrecid
    left outer join prod_class on gjae.partition = prod_class.partition 
    and gjae.ledgerdimension = prod_class.valuecombinationrecid
    left outer join trade_type on gjae.partition = trade_type.partition 
    and gjae.ledgerdimension = trade_type.valuecombinationrecid
    left join fintag tag on tag.recid = gjae.fin_tag
    left join account_string account_string on account_string.account_display_value = gjae.ledgeraccount
    where --fcp.type = 1 and 
    gje.accountingdate >= fcp.startdate 
    and gje.accountingdate <= fcp.enddate
    --and ((upper($$load_param) ='f') or (upper($$load_param)='i' 
    --and gje.accountingdate >= last_day(dateadd(month,$$month_param,current_date))))
   -- order by source_account_identifier, document_company, gl_date
)
select * from source

{% if day_today != full_load_day %}
    {% if not flags.FULL_REFRESH %}
    where source_date_updated >= current_date() - {{env_var('DBT_STD_INCR_LOOKBACK')}}
    {% endif %}
    {% endif %}
