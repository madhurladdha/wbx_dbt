{{
    config(
        tags = ["sls","sales","forecast","sls_forecast","finance","sls_finance"]
    )
}}
with mfg_cbom as (
    select * from {{ ref('fct_wbx_mfg_cbom')}}
),
cby as (
    select 
        distinct right(f1.version_id,4) as verison_year,f1.root_src_item_identifier,
        round(f1.root_src_unit_price,2) as root_src_unit_price,f1.eff_date,
        first_value(f2.eff_date) over (partition by right(f1.version_id,4),
        f1.root_src_item_identifier,round(f1.root_src_unit_price,2),f1.eff_date 
        order by abs(datediff(second,f1.eff_date,f2.eff_date))) as bl_eff_date
        ,case when len(reverse(cast(floor(reverse(abs(f2.root_src_unit_price))) as bigint))) < 2 
        then 2 else len(reverse(cast(floor(reverse(abs(f2.root_src_unit_price))) as bigint))) 
        end as numdecs
    from mfg_cbom f1
    inner join mfg_cbom f2 on right(f1.version_id,4) = right(f2.version_id,4) 
    and f1.root_src_item_identifier = f2.root_src_item_identifier
    and round(f1.root_src_unit_price,2) = round(f2.root_src_unit_price,2) 
    and upper(f2.stock_site) = 'WBX-BL'
    where upper(f1.stock_site) in('WBX-CBY') and f1.comp_bom_level <> 0
),
item_master as (
    select 
        im.source_item_identifier,max(case when nvl (upper(e.mangrpcd_site),'BL') 
        in('BL','BL/CBY','CBY','WEETABIX/ORG') then 'N' else 'Y' end) as mangrpcd_copack_flag 
    from {{ ref('dim_wbx_item')}} im
        left outer join {{ ref('dim_wbx_item_ext')}} e on im.item_guid = e.item_guid 
        where im.source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
        and is_real(try_to_numeric(im.source_item_identifier)) = 1 
        and len(im.source_item_identifier) = 5 
        group by im.source_item_identifier
),

x as (
    select bom.*,
        itm.mangrpcd_copack_flag,
        cby.bl_eff_date,
        nvl (cby.numdecs,2) as numdecs from mfg_cbom bom
        left outer join cby
        on right(bom.version_id,4) = cby.verison_year 
        and bom.root_src_item_identifier = cby.root_src_item_identifier 
        and round(bom.root_src_unit_price,2) = cby.root_src_unit_price and (bom.eff_date = cby.eff_date
        or bom.eff_date = cby.bl_eff_date)
        inner join item_master itm
        on bom.root_src_item_identifier = itm.source_item_identifier
        where ((upper(bom.stock_site) not in('WBX-CBY') and cby.root_src_item_identifier is null) 
        or (upper(bom.stock_site) = 'WBX-CBY' and comp_bom_level <> 0 
        and cby.root_src_item_identifier is not null)) and ((comp_bom_level = 1 and
        (upper(comp_calctype_desc) in('ITEM','SERVICE') or (upper(comp_calctype_desc) in ('BOM') 
        and upper(parent_item_indicator) in ('ITEM','PARENT')))) or (comp_bom_level = 0 
        and upper(comp_calctype_desc) in ('PRODUCTION') and upper(parent_item_indicator) = 'ITEM'))
),
sourcetable as (
    select 
        source_system,root_company_code,stock_site,version_id,
        nvl (x.bl_eff_date,x.eff_date) as eff_date,creation_date_time,expir_date,
        root_src_item_identifier,root_src_variant_code,x.numdecs,
        upper(case when x.comp_cost_group_id = '' and x.comp_bom_level = 0 then 'BI'
        when x.comp_cost_group_id = '' and x.comp_bom_level <> 0 then 'CO' 
        when upper(x.comp_cost_group_id) = 'MO' then 'CG' 
        when upper(x.comp_cost_group_id)  = 'TRAN' then 'CG' else x.comp_cost_group_id end) as comp_cost_group_id,
        max(root_src_unit_price) as root_src_unit_price,max(round(root_src_unit_price,numdecs)) as gl_unit_price,
        sum(case when x.comp_cost_group_id = '' and x.comp_bom_level = 0 
        then root_src_unit_price else comp_item_unit_cost end) as comp_item_unit_cost
    from x
    group by source_system,root_company_code,stock_site,version_id,nvl (x.bl_eff_date,x.eff_date),
    creation_date_time,expir_date,root_src_item_identifier,root_src_variant_code,x.numdecs,
    upper(case when x.comp_cost_group_id = '' and x.comp_bom_level = 0 then 'BI'
    when x.comp_cost_group_id = '' and x.comp_bom_level <> 0 then 'CO' 
    when upper(x.comp_cost_group_id) = 'MO' then 'CG' when upper(x.comp_cost_group_id)  = 'TRAN' 
    then 'CG' else x.comp_cost_group_id end)
),
cbom as (
    select 
        source_system,
        root_company_code,
        stock_site,
        version_id,
        eff_date,
        creation_date_time,
        expir_date,
        root_src_item_identifier,
        root_src_variant_code,
        root_src_unit_price,
        gl_unit_price
        ,case stock_site when 'WBX-CBY' then round(nvl ("'CG'",0),NumDecs) 
        else nvl ("'CG'",0) end                                     as raw_materials 
        ,case stock_site when 'WBX-cby' then round(nvl ("'CG_PACK'",0),NumDecs) 
        else nvl ("'CG_PACK'",0) end                                as packaging
        ,case stock_site when 'WBX-CBY' then round(nvl ("'LAB'",0),NumDecs) 
        else nvl ("'LAB'",0) end                                    as labour
        ,case stock_site when 'WBX-CBY' then round(nvl ("'BI'",0),NumDecs) 
        else nvl ("'BI'",0) end                                     as bought_in
        ,case stock_site when 'WBX-CBY' then round(nvl ("'CO'",0),NumDecs) 
        else NVL ("'CO'",0) end                                     as co_pack
        ,case stock_site when 'WBX-CBY' then round(gl_unit_price 
        - (round(round(nvl ("'CG'",0),NumDecs) + round(nvl ("'CG_PACK'",0),NumDecs) 
        + round(nvl ("'LAB'",0),NumDecs) + round(nvl ("'BI'",0),NumDecs) + 
        round(nvl ("'CO'",0),NumDecs),NumDecs)),NumDecs) else 0 end as other,
        case min(root_src_variant_code) over(partition by source_system,root_company_code,
        root_src_item_identifier order by eff_date desc range between current row 
        and unbounded following) when '' then 0 else 1 end          as variant_flag         
    from sourcetable x
   
PIVOT  
(  
sum(comp_item_unit_cost)  
for comp_cost_group_id in ('CG', 'CG_PACK','LAB', 'BI', 'CO','OTH')  
) as PivotTable
)
select
    source_system,
	root_company_code,
	stock_site,
	version_id,
	eff_date,
	creation_date_time,
	expir_date,
	root_src_item_identifier,
	root_src_variant_code,
	root_src_unit_price,
	gl_unit_price,
	raw_materials,
	packaging,
	labour,
	bought_in,
	co_pack,
	other,
	variant_flag
from cbom
