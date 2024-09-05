with d365_source as (
        select * from {{ source("D365", "wrk_ctr_table") }}  where _FIVETRAN_DELETED='FALSE' and upper(data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}
    ),

    renamed as (
        select
            'D365' as source,
            wrk_ctr_id as wrkctrid,
            name as name,
            wrk_ctr_type as wrkctrtype,
            effectivity_pct as effectivitypct,
            operation_sched_pct as operationschedpct,
            capacity as capacity,
            cap_unit as capunit,
            vend_id as vendid,
            created as created,
            queue_time_before as queuetimebefore,
            setup_time as setuptime,
            process_time as processtime,
            process_per_qty as processperqty,
            transp_time as transptime,
            queue_time_after as queuetimeafter,
            transfer_batch as transferbatch,
            to_hours as tohours,
            error_pct as errorpct,
            set_up_category_id as setupcategoryid,
            process_category_id as processcategoryid,
            bottleneck_resource as bottleneckresource,
            cap_limited as caplimited,
            capacity_batch as capacitybatch,
            property_limited as propertylimited,
            exclusive as exclusive,
            qty_category_id as qtycategoryid,
            route_group_id as routegroupid,
            wipissue_ledger_dimension as wipissueledgerdimension,
            wipvaluation_ledger_dimension as wipvaluationledgerdimension,
            resource_issue_ledger_dimension as resourceissueledgerdimension,
            resource_issue_offset_ledger_dimension as resourceissueoffsetledgerdim,
            default_dimension as defaultdimension,
            is_individual_resource as isindividualresource,
            worker as worker,
            null as pmfsequencegroupid,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid

        from d365_source

    )

select * from renamed
