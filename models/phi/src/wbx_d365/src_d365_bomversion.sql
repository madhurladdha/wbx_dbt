with
    d365_source as (
        select * from {{ source("D365", "bomversion") }}  where _FIVETRAN_DELETED='FALSE'
        ),

    renamed as (
        select
            'D365' as source,
            to_date as todate,
            from_date as fromdate,
            item_id as itemid,
            bomid as bomid,
            name as name,
            active as active,
            approved as approved,
            construction as construction,
            from_qty as fromqty,
            {{column_append('approver')}} as approver,
            invent_dim_id as inventdimid,
            pmf_formula_version_calculation as pmfformulaversioncalculation,
            pmf_total_cost_allocation as pmftotalcostallocation,
            pds_cwfrom_qty as pdscwfromqty,
            pds_cwsize as pdscwsize,
            pmf_batch_size as pmfbatchsize,
            pmf_bulk_parent as pmfbulkparent,
            pmf_co_by_var_allow as pmfcobyvarallow,
            pmf_formula_change_date as pmfformulachangedate,
            pmf_formula_multiple as pmfformulamultiple,
            pmf_type_id as pmftypeid,
            pmf_yield_pct as pmfyieldpct,
            modifieddatetime as modifieddatetime,
            null as del_modifiedtime,
            modifiedby as modifiedby,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid,
            null as wbxwrkctrgroupid,
            null as formularesourceid,
            null as useforcost
        from d365_source   where upper(dataareaid) in {{env_var("DBT_D365_COMPANY_FILTER")}}  

    )

select * from renamed