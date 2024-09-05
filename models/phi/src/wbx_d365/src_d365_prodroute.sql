with d365_source as (
        select *
        from {{ source("D365", "prod_route") }} where _FIVETRAN_DELETED='FALSE' AND upper(data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}
    ),

    renamed as (

        select
            'D365' as source,
            prod_id as prodid,
            opr_num as oprnum,
            level_ as level_,
            opr_num_next as oprnumnext,
            opr_id as oprid,
            queue_time_before as queuetimebefore,
            setup_time as setuptime,
            process_time as processtime,
            process_per_qty as processperqty,
            transp_time as transptime,
            queue_time_after as queuetimeafter,
            overlap_qty as overlapqty,
            error_pct as errorpct,
            acc_error as accerror,
            to_hours as tohours,
            transfer_batch as transferbatch,
            set_up_category_id as setupcategoryid,
            process_category_id as processcategoryid,
            opr_finished as oprfinished,
            formula_factor_1 as formulafactor1,
            route_type as routetype,
            backorder_status as backorderstatus,
            null as propertyid,
            route_group_id as routegroupid,
            qty_category_id as qtycategoryid,
            from_date as fromdate,
            from_time as fromtime,
            to_date as todate,
            to_time as totime,
            calc_qty as calcqty,
            calc_set_up as calcsetup,
            calc_proc as calcproc,
            opr_priority as oprpriority,
            formula as formula,             
            route_opr_ref_rec_id as routeoprrefrecid,             
            default_dimension as defaultdimension,
            link_type as linktype,
            opr_started_up as oprstartedup,
            executed_process as executedprocess,
            executed_setup as executedsetup,
            constant_released as constantreleased,
            phantom_bomfactor as phantombomfactor,
            wrk_ctr_id_cost as wrkctridcost,
            job_id_process as jobidprocess,
            job_id_setup as jobidsetup,
            job_pay_type as jobpaytype,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid

        from d365_source

    )

select * from renamed
