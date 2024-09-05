with source as (

    select * from {{ source('WEETABIX', 'bomversion') }}

),

renamed as (

    select
        todate,
        fromdate,
        itemid,
        bomid,
        name,
        active,
        approved,
        construction,
        fromqty,
        approver,
        inventdimid,
        pmfformulaversioncalculation,
        pmftotalcostallocation,
        pdscwfromqty,
        pdscwsize,
        pmfbatchsize,
        pmfbulkparent,
        pmfcobyvarallow,
        pmfformulachangedate,
        pmfformulamultiple,
        pmftypeid,
        pmfyieldpct,
        modifieddatetime,
        del_modifiedtime,
        modifiedby,
        dataareaid,
        recversion,
        partition,
        recid,
        wbxwrkctrgroupid,
        formularesourceid,
        useforcost

    from source

)

select * from renamed