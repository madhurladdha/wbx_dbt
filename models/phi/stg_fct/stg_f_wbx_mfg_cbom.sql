{{ config(
    tags=["manufacturing", "cbom","mfg_cbom","sales","cogs","pcos"]
) }}

{% set now = modules.datetime.datetime.now() %}
{%- set full_load_day -%} {{env_var('DBT_FULL_LOAD_DAY')}} {%- endset -%}
{%- set day_today -%} {{ now.strftime('%A') }} {%- endset -%}
with inventitemprice as (
    select * from {{ ref('src_inventitemprice')}}
),
inventtable as (
    select * from {{ ref('src_inventtable')}}
),
inventmodelgroupitem as (
    select * from {{ ref('src_inventmodelgroupitem')}}
),
ecoresproduct as (
    select * from {{ ref('src_ecoresproduct')}}
),
ecoresproducttranslation as (
    select * from {{ ref('src_ecoresproducttranslation')}}
),
ledger as (
    select * from {{ ref('src_ledger')}}
),
bomcalctable as (
    select * from {{ ref('src_bomcalctable')}}
),
inventdim as (
    select * from {{ ref('src_inventdim')}}
),
bomcalctrans as (
    select * from {{ ref('src_bomcalctrans')}}
),
bomcalctrans_tbl as (
    select * from {{ ref('stg_d_wbx_mfg_cbom_bomcalctrans_tbl')}}
),
max_icv as (
    select max(t1.createddatetime) as maxcreateddatetime,
    max(t1.activationdate) as maxactivationdate,
    t1.itemid as itemid,
    t1.versionid as versionid,
    t1.pricetype as pricetype,
    t1.inventdimid as inventdimid,
    t1.dataareaid as dataareaid,
    t1.partition as partition,
    1010 as recid 
    from inventitemprice t1
    where t1.pricetype = 0
    group by t1.itemid,t1.versionid,t1.pricetype,t1.inventdimid,t1.dataareaid,t1.partition),
 icv as (
    select iip.versionid,
    iip.itemid,
    erptproduct.name as description,
    iip.pricecalcid,
    iip.price,
    iip.dataareaid,
    iip.partition,
    iip.activationdate,
    iip.createddatetime,
    case when upper(imgi.modelgroupid) in('FG-STD','DRINKS') then
    nvl (dateadd(second,-1,lead (iip.createddatetime) over (partition by iip.itemid, nvl(id.inventsiteid,'') order by iip.itemid,
    nvl(id.inventsiteid,''),iip.createddatetime)),cast('2050-12-31' as datetime))
    else
    nvl (dateadd(second,-1,lead (iip.createddatetime) over (partition by iip.itemid,nvl(id.inventsizeid,''), nvl(id.inventsiteid,'') order by iip.itemid,nvl(id.inventsizeid,''),
    nvl(id.inventsiteid,''),iip.createddatetime)),cast('2050-12-31' as datetime))
    end as expir_date,
    iip.modifieddatetime,
    case when iip.createddatetime = mi.maxcreateddatetime and iip.activationdate = mi.maxactivationdate then 'yes' else 'no' end as active,
    nvl(id.inventsizeid,'') as variant,
    nvl(id.inventsiteid,'') as stock_site,
    l.accountingcurrency,
    case when iip.priceunit=0 then 0 else cast(iip.price/cast(iip.priceunit as float) as float) end as unit_price
    from inventitemprice iip 
    inner join inventtable it 
        on iip.dataareaid = it.dataareaid 
        and iip.itemid = it.itemid 
        and iip.partition = it.partition
    inner join inventmodelgroupitem imgi 
        on it.itemid = imgi.itemid 
        and it.dataareaid = imgi.itemdataareaid 
        and it.partition = imgi.partition
    inner join ecoresproduct erpproduct 
        on it.product = erpproduct.recid 
        and it.partition = erpproduct.partition
    inner join ecoresproducttranslation erptproduct 
        on erptproduct.product = erpproduct.recid 
        and erptproduct.partition = erptproduct.partition
    inner join max_icv mi 
        on iip.itemid = mi.itemid 
        and iip.inventdimid = mi.inventdimid 
        and iip.versionid = mi.versionid 
            and iip.dataareaid = mi.dataareaid 
        and iip.partition = mi.partition
    inner join ledger l 
        on upper(iip.dataareaid) = l.name
    left outer join bomcalctable bt 
        on iip.pricecalcid = bt.pricecalcid 
        and iip.dataareaid = bt.dataareaid 
        and iip.partition = bt.partition
    left outer join inventdim id 
        on iip.inventdimid = id.inventdimid 
        and iip.partition = id.partition 
        and iip.dataareaid = id.dataareaid
    where iip.activationdate <= to_date(convert_timezone('UTC',current_timestamp)) 
        and iip.pricetype = 0

 ),
  expbct 
    (source_bom_path,
     version_id,
     root_src_item_identifier,
     description,
     pricecalcid,
     active_flag,
     eff_date,
     creation_date_time,
     expir_date,
     source_updated_datetime,
     transaction_currency,
     comp_bom_level,
     comp_src_item_identifier,
	 comp_cost_group_id,
     calctype,
     comp_calctype_desc,
     consistofprice,
     consumptionvariable,
     costpriceqty,
     costprice,
     costpriceunit,
     root_company_code,
     partition,
	 consumption,
     qty,
     transaction_uom,
     exp_costpriceqty,
     exp_costprice,
     parent_item_indicator,
     invent_dim,source_business_unit_code,
     comp_src_variant_code,root_src_variant_code,
     stock_site,
     root_src_unit_price) 
    as 
    (
      select cast(concat(bct.pricecalcid, '-', cast(bct.linenum as varchar(255))) as varchar(255)),
    icv.versionid,
    icv.itemid,
    icv.description,
    icv.pricecalcid,
    icv.active,
    icv.createddatetime,
    icv.createddatetime,
    icv.expir_date,
    icv.modifieddatetime,
    icv.accountingcurrency,
    nvl(bct.level_,0) as level,
    nvl(bct.resource_,'') as material,
	nvl(bct.costgroupid,'') as costgroupid,
    nvl(bct.calctype,1) as calctype, 
    case nvl(bct.calctype,0) 
      when 0 then 'Production' when 1 then 'Item' when 2 then 'BOM' when 3 then 'Service' 
      when 4 then 'Setup' when 5 then 'Process' when 6 then 'Quantity'
	  when 7 then 'Surcharge' when 8 then 'Rate' when 9 then 'Cost Group' 
      when 10 then 'Output unit based' when 14 then 'Input unit based' when 15 then 'Purchase'
	  when 16 then 'Unit based purchase overheads' when 20 then 'Burden' when 21 then 'Batch'
	  else '' end as calctypedesc,
    nvl(bct.consistofprice,'') as consistofprice,
	nvl(cast(bct.consumptionvariable as float),0) as consumptionvariable,
    nvl(bct.costpriceqty,icv.price) as costpriceqty,
    nvl(bct.costprice,icv.price) as costprice,
    nvl(cast(bct.costpriceunit as float),1) as costpriceunit,
    icv.dataareaid,
    icv.partition,
    cast(nvl(bct.consumptionvariable, 0) as float) as consumption,
    cast(nvl(bct.qty, 0) as number(38, 10)) as qty, 
    nvl(bct.unitid,'Case') as unitid,
	cast(nvl(bct.costpriceqty, icv.price) as float) as exp_costpriceqty,
    cast(nvl(bct.costprice,icv.price) as float) as exp_costprice,
	case when nvl(bct.calctype, 1) in(0, 9) or nvl(bct.consistofprice,'') <> '' 
    then 'Parent' else 'Item' end as parentoritem,
    nvl(bct.inventdimstr,'') as invent_dim,
    nvl(id.inventlocationid,'') as plant,
    nvl(id.inventsizeid,'') as variant,
    icv.variant,
    icv.stock_site,
    icv.unit_price
    from icv 
    left outer join bomcalctrans_tbl bct 
        on icv.pricecalcid = bct.pricecalcid 
        and icv.dataareaid = bct.dataareaid 
        and icv.partition = bct.partition
 	left outer join inventdim id 
        on bct.inventdimid = id.inventdimid 
        and bct.partition = id.partition 
        and bct.dataareaid = id.dataareaid
	
    union all
    
    select cast(concat(eb.source_bom_path, '/', bct.pricecalcid, '-', cast(bct.linenum as varchar(4000))) as varchar(255)),
    eb.version_id,
    eb.root_src_item_identifier,
    eb.description,
    bct.pricecalcid,
    eb.active_flag,
    eb.eff_date,
    eb.creation_date_time,
    eb.expir_date,
    eb.source_updated_datetime,
    eb.transaction_currency,
    bct.level_ + eb.comp_bom_level as comp_bom_level,
    bct.resource_,
	bct.costgroupid,
    bct.calctype as calctype, 
    case bct.calctype 
	  when 0 then 'Production' when 1 then 'Item' when 2 then 'BOM' when 3 then 'Service' 
      when 4 then 'Setup' when 5 then 'Process' when 6 then 'Quantity'
	  when 7 then 'Surcharge' when 8 then 'Rate' when 9 then 'Cost Group' 
      when 10 then 'Output unit based' when 14 then 'Input unit based' when 15 then 'Purchase'
	  when 16 then 'Unit based purchase overheads' when 20 then 'Burden' when 21 then 'Batch'
	  else '' end as calctypedesc,
      bct.consistofprice as consistofprice,
	  nvl(cast(bct.consumptionvariable as float),0) as consumptionvariable,
      bct.costpriceqty,
      bct.costprice,
      nvl(cast(bct.costpriceunit as float),1) costpriceunit,
      bct.dataareaid,
      bct.partition,
      cast((nvl(bct.consumptionvariable,0) * nvl(eb.consumption,0) / nvl(eb.qty, 0)) as float) as consumption,
      cast(bct.qty as number(38, 10)) as qty,
      bct.unitid,
      cast((bct.costpriceqty * eb.consumption / nvl(eb.qty, 0)) as float) as exp_costpriceqty,
      cast((bct.costprice * eb.consumption / nvl(eb.qty, 0)) as float) as exp_costprice,
	  case when bct.calctype in(0, 9) or bct.consistofprice <> '' then 'Parent' else 'Item' end as parentoritem,
      bct.inventdimstr as invent_dim,
      eb.source_business_unit_code,
      eb.comp_src_variant_code,
      eb.root_src_variant_code,
      eb.stock_site,
      eb.root_src_unit_price
      from bomcalctrans bct 
	  inner join expbct eb 
        on eb.root_company_code = bct.dataareaid 
        and bct.partition = bct.partition 
        and bct.pricecalcid = eb.consistofprice
    ) 

   select 
    '{{ env_var("DBT_SOURCE_SYSTEM") }}'                                             as source_system,
        active_flag                                                                  as active_flag,
        stock_site                                                                   as stock_site,
        version_id                                                                   as version_id,
        source_bom_path                                                              as source_bom_path,
        eff_date                                                                     as eff_date,
        creation_date_time                                                           as creation_date_time,
        expir_date                                                                   as expir_date,
        source_updated_datetime                                                      as source_updated_datetime,
        transaction_currency                                                         as transaction_currency,
        transaction_uom                                                              as transaction_uom,
        root_company_code                                                            as root_company_code,
        root_src_item_identifier                                                     as root_src_item_identifier,
        root_src_variant_code                                                        as root_src_variant_code,
        root_src_unit_price                                                          as root_src_unit_price,
        case when comp_bom_level = 0 and comp_src_item_identifier = '' 
        then root_src_item_identifier else comp_src_item_identifier end              as comp_src_item_identifier,
        case when comp_bom_level = 0 and comp_src_item_identifier = '' 
        then root_src_variant_code else comp_src_variant_code end                    as comp_src_variant_code,
        consumption as comp_consumption_qty,qty as comp_consumption_unit,costprice   as comp_cost_price,costpriceunit as comp_cost_price_unit,
        case round(qty,4) when 0 then case comp_bom_level when 0 then costprice / 
        costpriceunit else 0 end else (cast(consumption as float) * costprice) / 
        (cast(qty * costpriceunit as float)) end                                     as comp_item_unit_cost,
        comp_bom_level                                                               as comp_bom_level,
        comp_calctype_desc                                                           as comp_calctype_desc,
        comp_cost_group_id                                                           as comp_cost_group_id,
        parent_item_indicator                                                        as parent_item_indicator,
        source_business_unit_code                                                    as source_business_unit_code
   from expbct 
   order by root_src_item_identifier,version_id,creation_date_time,eff_date,comp_bom_level

