
{{
    config(
        tags = ["wbx","manufacturing","work order","schedule"]
    )
}}

/* Model includes a few required changes found needed during D365 migration, BR1 specifically.
    Includes:
    -Changes in the default financial dimensions and how to capture them: ProductClass replaces Product and Sites replaces Site.
    -For the dimension Plant (Source BU), the code was changed to pull from the inventdim table rather than financial dims.
    -ProdStatus and ProdType code udpated a little due to how data is stored in D365
    -Employee joins changed to left join as some transactional data have createdby as a service account and not a real employee id.
*/

{% set curr_time = "substr(convert_timezone('UTC', current_timestamp()), 1, 10)" %}

/*adding this variable to remove redundant use of the statement in code down below.we will be using variable "{{curr_time}}" */


with hcmworker as (
    select * from {{ ref('src_hcmworker') }}
),

dirpartytable as (
    select * from {{ ref('src_dirpartytable') }}
),

dirpersonuser as (
    select * from {{ ref('src_dirpersonuser') }}
),

hcmpositionhierarchy as (
    select * from {{ ref('src_hcmpositionhierarchy') }}
),

hcmpositionworkerassignment as (
    select * from {{ ref('src_hcmpositionworkerassignment') }}
),

hcmposition as (
    select * from {{ ref('src_hcmposition') }}
),

wrkctrprodrouteactivity as (
    select * from {{ ref('src_wrkctrprodrouteactivity') }}
),

wrkctractivity as (
    select * from {{ ref('src_wrkctractivity') }}
),

wrkctractivityrequirementset as (
    select * from {{ ref('src_wrkctractivityrequirementset') }}
),

wrkctractivityrequirement as (
    select * from {{ ref('src_wrkctractivityrequirement') }}
),

wrkctractivityresourcerqurmnt as (
    select * from {{ ref('src_wrkctractivityresourcerqurmnt') }}
),

wrkctrtable as (
    select * from {{ ref('src_wrkctrtable') }}
),

inventtransoriginprodtable as (
    select * from {{ ref('src_inventtransoriginprodtable') }}
),

inventtransposting as (
    select * from {{ ref('src_inventtransposting') }}
),

inventtrans as (
    select * from {{ ref('src_inventtrans') }}
),

prodtable as (
    select * from {{ ref('src_prodtable') }}
),

srsanalysisenums as (
    select * from {{ ref('src_srsanalysisenums') }}
),

inventmodelgroupitem as (
    select * from {{ ref('src_inventmodelgroupitem') }}
),

inventdim as (
    select * from {{ ref('src_inventdim') }}
),

prodroute as (
    select * from {{ ref('src_prodroute') }}
),

dim_attributevaluesetitem_view as (
    select * from {{ ref('src_dim_attributevaluesetitem_view') }}
),

dimensionattribute as (
    select * from {{ ref('src_dimensionattribute') }}
),

inventtable as (
    select * from {{ ref('src_inventtable') }}
),

inventtablemodule as (
    select * from {{ ref('src_inventtablemodule') }}
),


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
    inner join
        hcmpositionworkerassignment as hpwa
        on hw.recid = hpwa.worker
    inner join hcmposition as hp on hpwa.position = hp.recid
    left outer join
        hcmpositionhierarchy as hph
        on hp.recid = hph.position
    -- Supervisor
    left outer join
        hcmposition as hp_s
        on hph.parentposition = hp_s.recid
    left outer join
        hcmpositionworkerassignment as hpwa_s
        on hpwa_s.position = hp_s.recid
    left outer join hcmworker as hw_s on hpwa_s.worker = hw_s.recid
    left outer join
        dirpartytable as dpt_s
        on hw_s.person = dpt_s.recid
    left outer join
        dirpersonuser as dpu_s
        on dpt_s.recid = dpu_s.personparty
    left outer join
        hcmpositionhierarchy as hph_s
        on hp_s.recid = hph_s.position
    -- Manager
    left outer join
        hcmposition as hp_m
        on hph_s.parentposition = hp_m.recid
    left outer join
        hcmpositionworkerassignment as hpwa_m
        on hpwa_m.position = hp_m.recid
    left outer join hcmworker as hw_m on hpwa_m.worker = hw_m.recid
    left outer join
        dirpartytable as dpt_m
        on hw_m.person = dpt_m.recid
    left outer join
        dirpersonuser as dpu_m
        on dpt_m.recid = dpu_m.personparty
    where {{curr_time}}
        between nvl( substr(hph.validfrom, 1, 10),dateadd(day,-1,{{curr_time}}) ) and nvl(substr(hph.validto, 1, 10), dateadd( day,1,{{curr_time}}) )
        and nvl(hph.positionhierarchytype, 5637145334) = 5637145334
        and {{curr_time}} between hpwa.validfrom and hpwa.validto
        and {{curr_time}} between nvl(substr(hph_s.validfrom, 1, 10),dateadd(day,-1,{{curr_time}})) and nvl(substr(hph_s.validto, 1, 10),dateadd(day,1,{{curr_time}}))
        and nvl(hph_s.positionhierarchytype, 5637145334) = 5637145334
        and {{curr_time}} between nvl(substr(hpwa_s.validfrom, 1, 10),dateadd(day,-1,{{curr_time}} )) and nvl(substr(hpwa_s.validto, 1, 10),dateadd( day, 1,{{curr_time}})) 
        and {{curr_time}} between nvl(substr(hpwa_m.validfrom, 1, 10),dateadd(day,-1,{{curr_time}}))  and nvl(substr(hpwa_m.validto, 1, 10),dateadd(day, 1,{{curr_time}}))
),

wrkctr as (
    select
        wcarr.wrkctrid,
        wct.name,
        wcpra.routedataareaid,
        wcpra.oprpriority,
        wcpra.oprnum,
        wcpra.prodid,
        wcpra.partition,
        wct.defaultdimension
    from wrkctrprodrouteactivity as wcpra
    inner join
        wrkctractivity as wca
        on
            wcpra.partition = wca.partition
            and wcpra.activity = wca.recid
    inner join
        wrkctractivityrequirementset as wcars
        on
            wca.recid = wcars.activity
            and wca.partition = wcars.partition
    inner join
        wrkctractivityrequirement as wcar
        on
            wcars.recid = wcar.activityrequirementset
            and wcar.partition = wcar.partition
    inner join
        wrkctractivityresourcerqurmnt as wcarr
        on
            wcar.partition = wcarr.partition
            and wcarr.activityrequirement = wcar.recid
    inner join
        wrkctrtable as wct
        on
            wcarr.wrkctrid = wct.wrkctrid
            and wcarr.partition = wct.partition
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
        on
            itopt.partition = itp.partition
            and itopt.prodorderdataareaid = itp.dataareaid
            and itopt.inventtransorigin = itp.inventtransorigin
            and itp.inventtranspostingtype = 1
    inner join
        inventtrans as it
        on
            itp.partition = it.partition
            and itp.dataareaid = it.dataareaid
            and itp.inventtransorigin = it.inventtransorigin
            and itp.voucher = it.voucher
            and itp.transdate = it.datefinancial
    where it.packingslipreturned = 0
),

source as (
    select
        cast(pt.prodid as varchar(255)) as work_order_number,
        cast(nvl(pt.prodtype, '0') as varchar(255)) as source_order_type_code,
        cast(nvl(pt.prodprio, 0) as varchar(255)) as priority_code,
        cast(nvl(trim(pt.prodstatus), ' ') as varchar(255)) as prodstatus,
        cast(ps.statusdesc as varchar(255)) as statusdesc,
        cast(nvl(trim(pt.itemid), ' ') as varchar(255))
            as source_item_identifier,
        nvl(id.inventsizeid, '') as variant,
        cast(nvl(trim(pt.dataareaid), ' ') as varchar(255)) as company_code,
        cast(wrkctr.wrkctrid as varchar(255)) as work_center_code,
        cast(wrkctr."NAME" as varchar(255)) as work_center_desc,
        --cast(dp.displayvalue as varchar(255)) as source_business_unit_code,
        cast(id.inventlocationid as varchar(255)) as source_business_unit_code,
        pt.bomid,
        pt.routeid,
        cast(nvl(trim(pt."NAME"), ' ') as varchar(255)) as description,
        pt.scheddate,
        pt.schedstart,
        pt.schedend,
        pt.dlvdate,
        pt.stupdate,
        pt.finisheddate,
        nvl(pt.qtysched, 0) as qtysched,
        pt.qtysched * it.netweight as scheduled_kg_qty,
        pt.qtystup,
        cast(to_char((pt.schedend), 'WW') as varchar(255)) as weekno,
        prd.typedesc,
        cast(nvl(trim(pt.createdby), ' ') as varchar(255)) as "USER",
        /* For D365 defaulting the following to Non-Employee as there is a service account user for the createdby value.    */
        cast(nvl(emps.manager,'Non-Employee') as varchar(255)) as manager,
        cast(nvl(emps.supervisor,'Non-Employee') as varchar(255)) as supervisor,
        cast(substr((pt.createddatetime), 1, 10) as date) as createddatetime,
        it.netweight as tran_kg_conv_factor,
        cast(nvl(trim(itm.unitid), ' ') as varchar(255)) as primary_uom,
        to_date({{curr_time}}) as snapshot_date,
        pt.realdate as gl_date,
        nvl(vdata.voucher, ' ') as voucher,
        pt.pmfconsordid as consolidated_batch_order,
        pt.pmfbulkord as bulk_flag,
        upper(imgi.modelgroupid) as item_model_group,
        cast(dprod.displayvalue as varchar(255)) as product,
        cast(dsite.displayvalue as varchar(255)) as site
    from prodtable as pt
    inner join
        (
            select
                enumitemvalue as prodstatus,
               /* For D365, the enumitemlabel values are not populated on 10-May-2024.  But the enumitemname is very similar so then defaulting to that.
                    This value is used later in the int models to filter so this is critical downstream.
                */
                nvl(enumitemlabel,enumitemname) as statusdesc,
            from srsanalysisenums
            where
                /*(languageid = 'en-gb') and */ (enumname = 'ProdStatus')
        /*Commenting out languageid filter as we don't have data for languageid in d365 src table. Avinash-052024*/
        ) as ps
        on pt.prodstatus = ps.prodstatus
    inner join
        (
            select
                enumitemvalue as prodtype,
                /* For D365, the enumitemlabel values are not populated on 10-May-2024.  But the enumitemname is very similar so then defaulting to that.
                    This value is used later in the int models to filter so this is critical downstream.
                */
                nvl(enumitemlabel,enumitemname) as typedesc
            from srsanalysisenums
            where
                /*(languageid = 'en-gb') and */ (enumname = 'ProdType')
        /*Commenting out languageid filter as we don't have data for languageid in d365 src table. Avinash-052024*/
        ) as prd
        on pt.prodtype = prd.prodtype
    inner join
        inventmodelgroupitem as imgi
        on
            pt.partition = imgi.partition
            and pt.dataareaid = imgi.itemdataareaid
            and pt.itemid = imgi.itemid
    left outer join
        inventdim as id
        on
            pt.partition = id.partition
            and pt.dataareaid = id.dataareaid
            and pt.inventdimid = id.inventdimid
    inner join
        prodroute as pr
        on
            pt.partition = pr.partition
            and pt.prodid = pr.prodid
            and pt.dataareaid = pr.dataareaid
    inner join wrkctr
        on
            pr.dataareaid = wrkctr.routedataareaid
            and pr.oprpriority = wrkctr.oprpriority
            and pr.oprnum = wrkctr.oprnum
            and pr.prodid = wrkctr.prodid
            and pr.partition = wrkctr.partition
    
        /* This subselect called dp was to get the dimensional plant value.  In D365 this dimension does not exist here.
        The plant can be retrieved the the inventdim (invent_dim) D365 table instead.
        */
    -- inner join
    --     (
    --         select
    --             displayvalue,
    --             dalvv.dimensionattributevalueset
    --         from dim_attributevaluesetitem_view as dalvv
    --         inner join
    --             dimensionattribute as da
    --             on dalvv.dimensionattribute = da.recid
    --         where da.name = 'Plant'
    --     ) as dp
    --     on wrkctr.defaultdimension = dp.dimensionattributevalueset
    inner join
        (
            select
                displayvalue,
                dalvv.dimensionattributevalueset
            from dim_attributevaluesetitem_view as dalvv
            inner join
                dimensionattribute as da
                on dalvv.dimensionattribute = da.recid
            where da.name = 'ProductClass'  --For D365, this hard-coded value changed from Product to ProductClass
        ) as dprod
        on pt.defaultdimension = dprod.dimensionattributevalueset
    inner join
        (
            select
                displayvalue,
                dalvv.dimensionattributevalueset
            from dim_attributevaluesetitem_view as dalvv
            inner join
                dimensionattribute as da
                on dalvv.dimensionattribute = da.recid
            where da.name = 'Sites'         --For D365, this hard-coded value changed from Site to Sites
        ) as dsite
        on pt.defaultdimension = dsite.dimensionattributevalueset

    inner join
        inventtable as it
        on
            pt.itemid = it.itemid
            and pt.dataareaid = it.dataareaid
            and pt.partition = it.partition
    inner join
        inventtablemodule as itm
        on
            pt.itemid = itm.itemid
            and pt.dataareaid = itm.dataareaid
            and pt.partition = itm.partition
            and itm.moduletype = 2
    /* For D365 changed to a left join as there is a service account user for the createdby value so join may not always be met.    */
    left outer join emps on pt.createdby = emps."USER"
    left outer join
        vdata
        on
            pt.partition = vdata.partition
            and pt.dataareaid = vdata.prodorderdataareaid
            and pt.prodid = vdata.prodorderid
    where
        (
            (upper(pt.dataareaid) = 'WBX')
            or (
                upper(pt.dataareaid) = 'RFL'
                and upper(substr(wrkctr.wrkctrid, -2, 2)) = 'PK'
            )
        )
        and (pt.prodstatus in (2, 3, 4, 5, 7))  -- 2 = Scheduled, 3 = Released, 4 = Started,5 = Reported as finished,7 = Ended
        and substr((pt.scheddate), 1, 10) <> to_date('1900-01-01', 'YYYY-MM-DD')
),

stage_table as (
    select
        work_order_number,
        source_order_type_code,
        priority_code,
        prodstatus,
        statusdesc,
        source_item_identifier,
        company_code,
        work_center_code,
        work_center_desc,
        source_business_unit_code,
        bomid,
        description,
        schedstart,
        schedend,
        dlvdate,
        stupdate,
        finisheddate,
        qtysched,
        scheduled_kg_qty,
        weekno,
        typedesc,
        user,
        manager,
        supervisor,
        createddatetime,
        tran_kg_conv_factor,
        primary_uom,
        snapshot_date,
        gl_date,
        voucher,
        consolidated_batch_order,
        bulk_flag,
        item_model_group,
        product,
        site
    from source
),

trans_stage as (
    select
        *,
        '{{ env_var("DBT_SOURCE_SYSTEM") }}' as v_source_system,
        v_source_system as source_system,
        current_timestamp() as load_date,
        current_timestamp() as update_date,
        current_date() as source_load_date,
        'CASE' as v_transaction_uom,
        v_transaction_uom as transaction_uom,
        schedend as planned_completion_date,
        createddatetime as order_date,
        schedstart as planned_start_date,
        dlvdate as requested_date,
        stupdate as actual_start_date,
        finisheddate as actual_completion_date,
        createddatetime as assigned_date,
        qtysched as v_scheduled_qty,
        v_scheduled_qty as scheduled_qty,
        0 as v_cancelled_qty,
        v_cancelled_qty as cancelled_qty,
        scheduled_kg_qty as o_scheduled_kg_qty,
        scheduled_kg_qty * 2.20462 as scheduled_lb_qty,
        tran_kg_conv_factor * 2.20462 as tran_lb_conv_factor,
        1 as tran_prim_conv_factor,
        ' ' as priority_desc
    from stage_table
),

final as (
    select
        snapshot_date as snapshot_date,
        planned_start_date as planned_start_date,
        planned_completion_date as planned_completion_date,
        source_order_type_code as source_order_type_code,
        typedesc as order_type_desc,
        work_order_number as work_order_number,
        null as related_document_type,
        null as related_document_number,
        null as related_line_number,
        priority_code as priority_code,
        priority_desc as priority_desc,
        description as description,
        company_code as company_code,
        source_business_unit_code as source_business_unit_code,
        prodstatus as status_code,
        statusdesc as status_desc,
        null as status_change_date,
        null as customer_address_number,
        user as originator_add_number,
        manager as manager_add_number,
        supervisor as supervisor_add_number,
        order_date as order_date,
        requested_date as requested_date,
        assigned_date as assigned_date,
        source_item_identifier as source_item_identifier,
        scheduled_qty as scheduled_qty,
        transaction_uom as transaction_uom,
        source_load_date as source_load_date,
        source_system as source_system,
        load_date as load_date,
        update_date as update_date,
        work_center_code as work_center_code,
        work_center_desc as work_center_desc,
        primary_uom as primary_uom,
        tran_prim_conv_factor as tran_prim_conv_factor,
        tran_lb_conv_factor as tran_lb_conv_factor,
        scheduled_lb_qty as scheduled_lb_qty,
        scheduled_kg_qty as scheduled_kg_qty,
        tran_kg_conv_factor as tran_kg_conv_factor,
        bomid as source_bom_identifier,
        consolidated_batch_order as consolidated_batch_order,
        voucher as voucher,
        item_model_group as item_model_group,
        product as product_class,
        site as site,
        gl_date as gl_date,
        bulk_flag as bulk_flag
    from trans_stage
)

select * from final


