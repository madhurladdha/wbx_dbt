{{
    config(
        tags=["wbx","manufacturing", "work order","produced",],
    )
}}

/* Model includes a few required changes found needed during D365 migration, BR1 specifically.
    Includes:
    -Changes in the default financial dimensions and how to capture them: ProductClass replaces Product and Sites replaces Site.
    -For the dimension Plant (Source BU), the code was changed to pull from the inventdim table rather than financial dims.
    -ProdStatus and ProdType code udpated a little due to how data is stored in D365
*/
with
	prodtable as (select * from {{ ref("src_prodtable") }}),
	srsanalysisenums as (select * from {{ ref("src_srsanalysisenums") }}),
	ps as  (
                select
                    enumitemvalue as prodstatus,
                    /* For D365, the enumitemlabel values are not populated on 10-May-2024.  But the enumitemname is very similar so then defaulting to that.
                        This value is used later in the int models to filter so this is critical downstream.
                    */
                    nvl(enumitemlabel,enumitemname) as statusdesc,
                from srsanalysisenums
                where
                    /*(languageid = 'en-gb') and */ (enumname = 'ProdStatus')
                /*Commenting out this filter as we don't have data for languageid in d365 src table. Avinash-052024*/
            ),
	prd as  (
                select
                    enumitemvalue as prodtype,
                    /* For D365, the enumitemlabel values are not populated on 10-May-2024.  But the enumitemname is very similar so then defaulting to that.
                        This value is used later in the int models to filter so this is critical downstream.
                    */
                    nvl(enumitemlabel,enumitemname) as typedesc
                from srsanalysisenums
                where
                    /* (languageid = 'en-gb') and */ (enumname = 'ProdType')
                /*Commenting out this filter as we don't have data for languageid in d365 src table. Avinash-052024*/
            ),
    hcmworker as (select * from {{ ref("src_hcmworker") }}),
    dirpartytable as (select * from {{ ref("src_dirpartytable") }}),
    hcmpositionworkerassignment as (
        select * from {{ ref("src_hcmpositionworkerassignment") }}
    ),
    hcmposition as (select * from {{ ref("src_hcmposition") }}),
    hcmpositionhierarchy as (
        select * from {{ ref("src_hcmpositionhierarchy") }}
    ),
    dirpersonuser as (select * from {{ ref("src_dirpersonuser") }}),
    inventtransoriginprodtable as (
        select * from {{ ref("src_inventtransoriginprodtable") }}
    ),
    inventtransposting as (select * from {{ ref("src_inventtransposting") }}),
    inventtrans as (select * from {{ ref("src_inventtrans") }}),
    prodroutejob as (select * from {{ ref("src_prodroutejob") }}),
    
    
    inventmodelgroupitem as (
        select * from {{ ref("src_inventmodelgroupitem") }}
    ),
    inventdim as (select * from {{ ref("src_inventdim") }}),
    prodtablejour as (select * from {{ ref("src_prodtablejour") }}),
    prodroute as (select * from {{ ref("src_prodroute") }}),
    wrkctrprodrouteactivity as (
        select * from {{ ref("src_wrkctrprodrouteactivity") }}
    ),
    wrkctractivity as (select * from {{ ref("src_wrkctractivity") }}),
    wrkctractivityrequirementset as (
        select * from {{ ref("src_wrkctractivityrequirementset") }}
    ),
    wrkctractivityrequirement as (
        select * from {{ ref("src_wrkctractivityrequirement") }}
    ),
    wrkctractivityresourcerqurmnt as (
        select * from {{ ref("src_wrkctractivityresourcerqurmnt") }}
    ),
    wrkctrtable as (select * from {{ ref("src_wrkctrtable") }}),
	dim_attributevaluesetitem_view as (
	    select * from {{ ref("src_dim_attributevaluesetitem_view") }}
	),
	dimensionattribute as (select * from {{ ref("src_dimensionattribute") }}),
	
    /* This subselect called dp was to get the dimensional plant value.  In D365 this dimension does not exist here.
        The plant can be retrieved the the inventdim (invent_dim) D365 table instead.
    */

--     dp as  (
--                 select
-- displayvalue,
-- dalvv.dimensionattributevalueset 
--                 from dim_attributevaluesetitem_view as dalvv
--                 inner join
--                     dimensionattribute as da
--                     on dalvv.dimensionattribute = da.recid
          
--                 where da.name = 'Plant'
--             ),

	dprod as    (
                select
displayvalue,
dalvv.dimensionattributevalueset
                from dim_attributevaluesetitem_view as dalvv
                inner join
                    dimensionattribute as da
                    on dalvv.dimensionattribute = da.recid
            
                where da.name = 'ProductClass'  --For D365, this hard-coded value changed from Product to ProductClass
            ),
	dsite as   (
                select
displayvalue,
dalvv.dimensionattributevalueset
                from dim_attributevaluesetitem_view as dalvv
                inner join
                    dimensionattribute as da
                      on dalvv.dimensionattribute = da.recid
                    
                where da.name = 'Sites'         --For D365, this hard-coded value changed from Site to Sites
            ),
    
    inventtable as (select * from {{ ref("src_inventtable") }}),
    inventtablemodule as (select * from {{ ref("src_inventtablemodule") }}),
    dim_date as (select * from {{ ref("src_dim_date") }}),

    
    emps as (
        select
            dpt."NAME" as name_user,
            dpu.user_ as "USER",
            dpt_s."NAME" as name_supervisor,
            dpu_s.user_ as supervisor,
            dpt_m."NAME" as name_manager,
            dpu_m.user_ as manager
        from hcmworker as hw
        inner join dirpartytable as dpt on hw.person = dpt.recid
      
      inner join dirpersonuser as dpu on dpt.recid = dpu.personparty 
        inner join hcmpositionworkerassignment as hpwa on hw.recid = hpwa.worker
        inner join hcmposition as hp on hpwa.position = hp.recid
        left outer join hcmpositionhierarchy as hph on hp.recid = hph.position
        -- Supervisor
        left outer join hcmposition as hp_s on hph.parentposition = hp_s.recid
        left outer join
            hcmpositionworkerassignment as hpwa_s
            on hpwa_s.position = hp_s.recid
        left outer join hcmworker as hw_s on hpwa_s.worker = hw_s.recid
        left outer join dirpartytable as dpt_s on hw_s.person = dpt_s.recid
        left outer join
            dirpersonuser as dpu_s
        on dpt_s.recid = dpu_s.personparty
        left outer join
            hcmpositionhierarchy as hph_s
        on hp_s.recid = hph_s.position
        -- Manager
        left outer join hcmposition as hp_m on hph_s.parentposition = hp_m.recid
        left outer join
            hcmpositionworkerassignment as hpwa_m
            on hpwa_m.position = hp_m.recid
        left outer join hcmworker as hw_m on hpwa_m.worker = hw_m.recid
        left outer join dirpartytable as dpt_m on hw_m.person = dpt_m.recid
        left outer join
            dirpersonuser as dpu_m
        on dpt_m.recid = dpu_m.personparty
        where
            current_date between nvl(hph.validfrom, current_date - 1) and nvl(
                hph.validto, current_date + 1
            )
            and nvl(hph.positionhierarchytype, 5637145334) = 5637145334
            and current_date between hpwa.validfrom and hpwa.validto
            and current_date between nvl(
                hph_s.validfrom, current_date - 1
            ) and nvl(
                hph_s.validto, current_date + 1
            )
            and nvl(hph_s.positionhierarchytype, 5637145334) = 5637145334
            and current_date between nvl(
                hpwa_s.validfrom, current_date - 1
            ) and nvl(
                hpwa_s.validto, current_date + 1
            )
            and current_date between nvl(
                hpwa_m.validfrom, current_date - 1
            ) and nvl(
                hpwa_m.validto, current_date + 1
            )
    ),
    vdata as (
        select distinct
            itopt.prodorderdataareaid,
            itopt.prodorderid,
            it.partition,
            it.dataareaid,
            it.voucher
        from inventtransoriginprodtable as itopt
        inner join
            inventtransposting as itp
            on itopt.partition = itp.partition
            and itopt.prodorderdataareaid = itp.dataareaid
            and itopt.inventtransorigin = itp.inventtransorigin
            and itp.inventtranspostingtype = 1
        inner join
            inventtrans as it
            on itp.partition = it.partition
            and itp.dataareaid = it.dataareaid
            and itp.inventtransorigin = it.inventtransorigin
            and itp.voucher = it.voucher
            and itp.transdate = it.datefinancial
        where it.packingslipreturned = 0
    ),
    pj as (
        select distinct
partition,
dataareaid,
prodid,
wrkctrid
        from prodroutejob
        where
            dataareaid = ('WBX')
            or (
                dataareaid in {{ env_var("DBT_D365_COMPANY_FILTER") }}
                and right(wrkctrid, 2) = 'PK'
            )
    ),
    source as (
        select
            cast(pt.prodid as varchar(255)) as work_order_number,
            cast(nvl(pt.prodtype, '0') as varchar(255))
                as source_order_type_code,
            pt.prodprio as prodprio,
            pt.prodstatus,
            cast(nvl(pt.dataareaid, ' ') as varchar(255)) as company_code,
            cast(ps.statusdesc as varchar(255)) as status_desc,
            cast(nvl(pt.itemid, ' ') as varchar(255)) as source_item_identifier,
            nvl(id.inventsizeid, '') as variant,
            cast(
                coalesce(wcarr.wrkctrid, pj.wrkctrid, '') as varchar(255)
            ) as work_center_code,  -- *******
            cast(nvl(wct."NAME", '') as varchar(255)) as work_center_desc,  
            --cast(dp.displayvalue as varchar(255)) as source_business_unit_code,
            cast(id.inventlocationid as varchar(255)) as source_business_unit_code,
            pt.bomid,
            pt.routeid,
            cast(nvl(pt."NAME", ' ') as varchar(255)) as description,
            case
                when
                    substr((pt.scheddate), 1, 10)
                    = to_date('1900-01-01', 'YYYY-MM-DD')
                then pt.latestscheddate
                else pt.scheddate
            end as scheddate,
            pt.schedstart,
            pt.schedend,
            pt.dlvdate,
            pt.stupdate,
            pt.finisheddate,
            pt.qtysched,
            pt.qtysched * it.netweight as scheduled_kg_qty,
            pt.qtystup,
            pt.remaininventphysical,
            weekofyear(pt.schedend) as weekno,
            cast(prd.typedesc as varchar2(255 byte)) as order_type_desc,
            sum(nvl(pdj.qtygood, 0)) as produced_qty,
            sum(nvl(pdj.qtygood, 0))
            * cast(it.netweight as decimal(38, 10)) as produced_kg_qty,
            cast(pt.createdby as varchar(255)) as wo_creator_add_number,
            cast(nvl(emps.manager, pt.createdby) as varchar(255))
                as manager_add_number,
            cast(
                nvl(emps.supervisor, pt.createdby) as varchar(255)
            ) as supervisor_add_number,
            pt.createddatetime,
            it.netweight as tran_kg_conv_factor,
            cast(nvl(itm.unitid, ' ') as varchar(255)) as primary_uom,
            pt.realdate,
            nvl(vdata.voucher, ' ') as voucher,
            pt.pmfconsordid,
            pt.pmfbulkord,
            upper(imgi.modelgroupid) as item_model_group,
            cast(dprod.displayvalue as varchar(255)) as product,
            cast(dsite.displayvalue as varchar(255)) as site,
            id.inventsiteid as stock_site
        from prodtable as pt

        inner join
        ps on pt.prodstatus = ps.prodstatus

        inner join prd on pt.prodtype = prd.prodtype

        inner join
            inventmodelgroupitem as imgi
            on pt.partition = imgi.partition
            and pt.dataareaid = imgi.itemdataareaid
            and pt.itemid = imgi.itemid
        inner join
            inventdim as id
            on pt.partition = id.partition
            and pt.dataareaid = id.dataareaid
            and pt.inventdimid = id.inventdimid  
        inner join
            prodtablejour as pdj
            on pt.partition = pdj.partition
            and pt.dataareaid = pdj.dataareaid
            and pt.prodid = pdj.prodid
            and pt.itemid = pdj.itemid
            and pdj.journaltype = 2 
        left outer join
            prodroute as pr
            on pt.partition = pr.partition
            and pt.prodid = pr.prodid
            and pt.dataareaid = pr.dataareaid  
        left outer join
            pj
            on pt.partition = pj.partition
            and pt.prodid = pj.prodid
            and pt.dataareaid = pj.dataareaid  

        left outer join
            wrkctrprodrouteactivity as wcpra
            on pr.dataareaid = wcpra.routedataareaid
            and pr.oprpriority = wcpra.oprpriority
            and pr.oprnum = wcpra.oprnum
            and pr.prodid = wcpra.prodid
            and pr.partition = wcpra.partition  
        left outer join
            wrkctractivity as wca
            on wcpra.partition = wca.partition
            and wcpra.activity = wca.recid  
        left outer join
            wrkctractivityrequirementset as wcars
            on wca.recid = wcars.activity
            and wca.partition = wcars.partition  
        left outer join
            wrkctractivityrequirement as wcar
            on wcars.recid = wcar.activityrequirementset
            and wcars.partition = wcar.partition
            and wcar.relationshiptype = 1  
        left outer join
            wrkctractivityresourcerqurmnt as wcarr
            on wcar.partition = wcarr.partition
            and wcarr.activityrequirement = wcar.recid  
        left outer join
            wrkctrtable as wct
            on nvl(wcarr.wrkctrid, pj.wrkctrid) = wct.wrkctrid
            and nvl(wcarr.partition, pj.partition) = wct.partition  

        /* The Plant dimensional value is now retrieved from invent_dim table in D365.  */
        -- inner join
        --    dp
        --     on pt.defaultdimension = dp.dimensionattributevalueset  
        inner join
         dprod
            on pt.defaultdimension = dprod.dimensionattributevalueset
        inner join
           dsite
            on pt.defaultdimension = dsite.dimensionattributevalueset
        inner join
            inventtable as it
            on pt.itemid = it.itemid
            and pt.dataareaid = it.dataareaid
            and pt.partition = it.partition
        inner join
            inventtablemodule as itm
            on pt.itemid = itm.itemid
            and pt.dataareaid = itm.dataareaid
            and pt.partition = itm.partition
            and itm.moduletype = 2
        left outer join emps on pt.createdby = emps."USER" 
        left outer join
            vdata
            on pt.partition = vdata.partition
            and pt.dataareaid = vdata.prodorderdataareaid
            and pt.prodid = vdata.prodorderid

        where
            (
                (upper(pt.dataareaid) = 'WBX')
                or (
                    upper(
                        pt.dataareaid
                    ) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
                    and nvl(pr.oprpriority, 0) = 0
                )
            )
            and (pt.prodstatus in (2, 3, 4, 5, 7))  -- 2 = Scheduled, 3 = Released, 4 = Started,5 = Reported as finished,7 = Ended
           
            and (
                substr((pt.scheddate), 1, 10)
                <> to_date('1900-01-01', 'YYYY-MM-DD')
                or substr((pt.latestscheddate), 1, 10)
                <> to_date('1900-01-01', 'YYYY-MM-DD')
            )
        group by
            pt.prodid,
            pt.prodtype,
            pt.prodprio,
            pt.prodstatus,
            pt.schedend,
            ps.statusdesc,
            pt.dataareaid,
            pt.itemid,
            id.inventsizeid,
            pt.bomid,
            pt.routeid,
            pt."NAME",
            pt.schedstart,
            pt.schedend,
            pt.dlvdate,
            pt.stupdate,
            pt.finisheddate,
            pt.qtysched,
            pt.qtystup,
            pt.remaininventphysical,
            weekofyear(pt.schedend),
            prd.typedesc,
            coalesce(wcarr.wrkctrid, pj.wrkctrid, ''),
            wct."NAME",
            --dp.displayvalue,
            id.inventlocationid,
            pt.createdby,
            emps.manager,
            emps.supervisor,
            pt.createddatetime,
            case
                when
                    substr((pt.scheddate), 1, 10)
                    = to_date('1900-01-01', 'YYYY-MM-DD')
                then pt.latestscheddate
                else pt.scheddate
            end,
            it.netweight,
            itm.unitid,
            pt.realdate,
            pt.pmfconsordid,
            pt.pmfbulkord,
            upper(imgi.modelgroupid),
            pt.realdate,
            vdata.voucher,
            dprod.displayvalue,
            dsite.displayvalue,
            id.inventsiteid
        having sum(nvl(pdj.qtygood, 0)) > 0
        order by pt.prodid, pt.schedend
    ),
tfm as (
select 
    '{{ env_var("DBT_SOURCE_SYSTEM") }}'      as source_system,
            s.work_order_number as work_order_number,
            s.source_order_type_code as source_order_type_code,
            iff( s.prodprio is null, 0, s.prodprio ) as priority_code,
            iff(s.prodstatus is null,0,s.prodstatus) as status_code,
            s.company_code as company_code,
            s.status_desc as status_desc,
            null as status_change_date,
            null as source_customer_code,
            pc.calendar_date as planned_completion_date,
            ps.calendar_date as planned_start_date,
            od.calendar_date as order_date,
            ad.calendar_date as actual_start_date,
            ac.calendar_date as actual_completion_date,
            sd.calendar_date as assigned_date,
            rd.calendar_date as requested_date,
            0 as cancelled_qty,
            s.source_item_identifier as source_item_identifier,
            s.variant as variant,
            s.work_center_code as work_center_code,  
            s.work_center_desc as work_center_desc, 
            s.source_business_unit_code as source_business_unit_code,
            s.bomid as source_bom_identifier,
            s.routeid,
            s.description as description,
            s.scheddate,
            s.schedstart as schedstart,
            s.schedend as schedend,
            s.dlvdate as dlvdate,
            s.stupdate,
            s.finisheddate,
            s.qtysched as scheduled_qty,
            s.scheduled_kg_qty as scheduled_kg_qty,
            scheduled_kg_qty * 2.20462 as scheduled_lb_qty,
            s.qtystup,
            s.remaininventphysical,
            s.weekno as weekno,
            s.order_type_desc as order_type_desc ,
            null as related_document_type,
            null as related_document_number,
            null as related_line_number,
            'CASE' as transaction_uom,
            current_timestamp() as source_load_date,
            s.produced_qty as produced_qty,
            s.produced_kg_qty as produced_kg_qty,
            produced_kg_qty * 2.20462	as produced_lb_qty,
            s.wo_creator_add_number as wo_creator_add_number,
            s.manager_add_number as manager_add_number,
            s.supervisor_add_number as supervisor_add_number,
            s.createddatetime as createddatetime,
            s.tran_kg_conv_factor as tran_kg_conv_factor,
            tran_kg_conv_factor * 2.20462	as tran_lb_conv_factor,
            1 as tran_prim_conv_factor,
            ' ' as priority_desc, 
            s.primary_uom as primary_uom,
            s.realdate as gl_date,
            s.voucher as voucher,
            s.pmfconsordid as consolidated_batch_order,
            s.pmfbulkord as bulk_flag,
            s.item_model_group as item_model_group,
            s.product as product_class,
            s.site as site,
            s.stock_site as stock_site,
            current_timestamp() as load_date,
            current_timestamp() as update_date

from source as s
left outer join dim_date as pc on pc.calendar_date = s.schedend
left outer join
    dim_date as od
on od.calendar_date = trunc(s.createddatetime,'dd')
left outer join dim_date as ps on ps.calendar_date = s.schedstart
left outer join dim_date as rd on rd.calendar_date = s.dlvdate
left outer join dim_date as ad on ad.calendar_date = s.stupdate
left outer join dim_date as ac on ac.calendar_date = s.finisheddate
left outer join
    dim_date as sd
on sd.calendar_date = trunc(s.createddatetime,'dd')

)
select * from tfm 