with d365_source as (
        select *
        from {{ source("D365", "proj_budget_line") }} where _FIVETRAN_DELETED='FALSE'
    ),

    renamed as (


        select
            'D365' as source,
            proj_allocation_method as projallocationmethod,
            category_id as categoryid,
            proj_trans_type as projtranstype,
            original_budget as originalbudget,
            committed_revisions as committedrevisions,
            uncommitted_revisions as uncommittedrevisions,
            total_budget as totalbudget,
            proj_budget_line_type as projbudgetlinetype,             
            proj_budget as projbudget,
            null as activitynumber,
            proj_id as projid,
            modifieddatetime as modifieddatetime,
            modifiedby as modifiedby,
            createddatetime as createddatetime,
            createdby as createdby,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid
        from d365_source where upper(dataareaid) in {{env_var("DBT_D365_COMPANY_FILTER")}}

    )

select * from renamed