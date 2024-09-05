with d365_source as (
        select *
        from {{ source("D365", "req_po") }}
        where _FIVETRAN_DELETED='FALSE' AND trim(upper(data_area_id)) in {{env_var("DBT_D365_COMPANY_FILTER")}}
    ),

renamed as (

    
    select 
        'D365' as source,
        item_id as itemid,
        route_jobs_updated as routejobsupdated,
        req_date as reqdate,
        qty as qty,
        req_date_order as reqdateorder,
        vend_id as vendid,
        item_group_id as itemgroupid,
        req_postatus as reqpostatus,
        purch_unit as purchunit,         
        plan_version as planversion,
        sched_method as schedmethod,
        null as purchid,
        req_date_dlv as reqdatedlv,
        ref_id as refid,
        ref_type as reftype,
        item_route_id as itemrouteid,
        item_bom_id as itembomid,
        item_buyer_group_id as itembuyergroupid,
        cov_invent_dim_id as covinventdimid,
        req_time_order as reqtimeorder,
        vend_group_id as vendgroupid,
        lead_time as leadtime,
        calendar_days as calendardays,
        sched_to_date as schedtodate,
        sched_from_date as schedfromdate,
        purch_qty as purchqty,
        req_time as reqtime,
        bomroute_created as bomroutecreated,
        is_derived_directly as isderiveddirectly,
        is_forecast_purch as isforecastpurch,
        intv_qr as intvqr,
        intv_mth as intvmth,
        intv_wk as intvwk,
        cost_amount as costamount,
        null as transferid,         
        product as product,
        pds_cwreq_qty as pdscwreqqty,
        pmf_bulk_ord as pmfbulkord,
        null as pmfplanningitemid,
        pmfsequenced as pmfsequenced,
        pmf_yield_pct as pmfyieldpct,
        modifieddatetime as modifieddatetime,
        null as del_modifiedtime,
        modifiedby as modifiedby,
        upper(data_area_id) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid,
        null as wbxreqdlvtime,
        null as prodimported
    from d365_source

)
select * from renamed 