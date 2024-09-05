with d365_source as (
        select *
        from {{ source("D365", "req_trans") }}
        where _FIVETRAN_DELETED='FALSE' AND trim(upper(data_area_id)) in {{env_var("DBT_D365_COMPANY_FILTER")}}
    ),
renamed as (

    
    select 
        'D365' as source,
        item_id as itemid,
        cov_invent_dim_id as covinventdimid,
        req_date as reqdate,
        direction as direction,
        ref_type as reftype,
        open_status as openstatus,
        qty as qty,
        cov_qty as covqty,
        ref_id as refid,
        keep as keep,
        req_date_dlv_orig as reqdatedlvorig,
        futures_days as futuresdays,
        futures_marked as futuresmarked,
        opr_num as oprnum,
        action_qty_add as actionqtyadd,
        action_days as actiondays,
        action_marked as actionmarked,
        action_type as actiontype,         
        plan_version as planversion,
        original_quantity as originalquantity,
        is_derived_directly as isderiveddirectly,
        priority as priority,
        action_date as actiondate,
        futures_date as futuresdate,         
        invent_trans_origin as inventtransorigin,         
        bomref_rec_id as bomrefrecid,         
        marking_ref_invent_trans_origin as markingrefinventtransorigin,
        level_ as level_,
        bomtype as bomtype,
        item_route_id as itemrouteid,
        item_bom_id as itembomid,
        is_forecast_purch as isforecastpurch,         
        last_plan_rec_id as lastplanrecid,
        req_time as reqtime,
        futures_time as futurestime,
        supply_demand_sub_classification as supplydemandsubclassification,
        req_process_id as reqprocessid,
        intercompany_planned_order as intercompanyplannedorder,
        pmf_plan_group_primary_issue as pmfplangroupprimaryissue,
        cust_account_id as custaccountid,
        cust_group_id as custgroupid,
        is_delayed as isdelayed,
        mcrprice_time_fence as mcrpricetimefence,
        pds_expiry_date as pdsexpirydate,
        pds_sellable_days as pdssellabledays,
        pmf_action_qty_add as pmfactionqtyadd,
        pmf_co_by_ref_rec_id as pmfcobyrefrecid,
        null as pmfplangroupid,
        pmf_plan_group_priority as pmfplangrouppriority,
        null as pmfplanningitemid,
        pmf_plan_priority_current as pmfplanprioritycurrent,
        requisition_line as requisitionline,
        upper(data_area_id) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid,
        is_forced_item_bom_id as isforceditembomid,
        is_forced_item_route_id as isforceditemrouteid,
        futures_calculated as futurescalculated
    from d365_source

)

select * from renamed
