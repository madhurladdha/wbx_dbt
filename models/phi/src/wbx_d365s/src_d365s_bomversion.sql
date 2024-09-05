with
d365_source as (
    select *
    from {{ source("D365S", "bomversion") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (
    select
        'D365S' as source,
        cast(todate as TIMESTAMP_NTZ) as todate,
        cast(fromdate as TIMESTAMP_NTZ) as fromdate,
        itemid as itemid,
        bomid as bomid,
        name as name,
        active as active,
        approved as approved,
        construction as construction,
        fromqty as fromqty,
        {{ column_append('approver') }} as approver,
        inventdimid as inventdimid,
        pmfformulaversioncalculation as pmfformulaversioncalculation,
        pmftotalcostallocation as pmftotalcostallocation,
        pdscwfromqty as pdscwfromqty,
        pdscwsize as pdscwsize,
        pmfbatchsize as pmfbatchsize,
        pmfbulkparent as pmfbulkparent,
        pmfcobyvarallow as pmfcobyvarallow,
        cast(pmfformulachangedate as TIMESTAMP_NTZ) as pmfformulachangedate,
        pmfformulamultiple as pmfformulamultiple,
        pmftypeid as pmftypeid,
        pmfyieldpct as pmfyieldpct,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        null as del_modifiedtime,
        modifiedby as modifiedby,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid,
        null as wbxwrkctrgroupid,
        null as formularesourceid,
        null as useforcost
    from d365_source
    where upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}

)

select * from renamed