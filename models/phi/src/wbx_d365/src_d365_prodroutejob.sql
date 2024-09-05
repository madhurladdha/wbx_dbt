with d365_source as (
        select *
        from {{ source("D365", "prod_route_job") }}  where _FIVETRAN_DELETED='FALSE' AND upper(data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}

    ),

    renamed as (

   select
            'D365' as source,
            prod_id as prodid,
            opr_num as oprnum,
            num_type as numtype,
            job_type as jobtype,
            link as link,
            link_type as linktype,
            opr_priority as oprpriority,
            sched_cancelled as schedcancelled,
            job_control as jobcontrol,
            wrk_ctr_id as wrkctrid,
            from_date as fromdate,
            from_time as fromtime,
            to_date as todate,
            to_time as totime,
            job_status as jobstatus,
            null as propertyid,
            executed_pct as executedpct,
            job_id as jobid,
            realized_start_date as realizedstartdate,
            realized_start_time as realizedstarttime,
            realized_end_date as realizedenddate,
            realized_end_time as realizedendtime,
            num_secondary as numsecondary,
            num_primary as numprimary,
            sched_time_hours as schedtimehours,
            calc_time_hours as calctimehours,
            job_finished as jobfinished,
            job_pay_type as jobpaytype,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid

        from d365_source

    )

select * from renamed
