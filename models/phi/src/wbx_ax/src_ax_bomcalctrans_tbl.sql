{{ config(materialized=env_var("DBT_MAT_TABLE"),tags=["sales","budget"]) }}
--change this source materialization to table. Since this is being used in the view v_sls_wtx_budget_pcos_projections
--as recursive cte. Keeping it as view was throwing SQL execution internal error: Processing aborted due to error 300010:423728544; incident 9425260.
with source as (

    select * from {{ source('WEETABIX', 'bomcalctrans') }}

),

renamed as (

    select
        costgroupid,
        level_,
        qty,
        costprice,
        costmarkup,
        salesprice,
        salesmarkup,
        transdate,
        linenum,
        resource_,
        unitid,
        oprid,
        inventdimstr,
        consumptionvariable,
        consumptionconstant,
        bom,
        oprnum,
        calctype,
        costpriceunit,
        costpriceqty,
        salespriceqty,
        costmarkupqty,
        salesmarkupqty,
        pricecalcid,
        numofseries,
        oprnumnext,
        oprpriority,
        consumptioninvent,
        inventdimid,
        vendid,
        consumptype,
        salespriceunit,
        netweightqty,
        infolog,
        salespricemodelused,
        pricediscqty,
        costpricemodelused,
        calcgroupid,
        costpricefallbackversion,
        salespricefallbackversion,
        routelevel,
        costpriceqtyseccur_ru,
        costmarkupqtyseccur_ru,
        costpriceseccur_ru,
        costmarkupseccur_ru,
        consistofprice,
        parentbomcalctrans,
        costcalculationmethod,
        createddatetime,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
