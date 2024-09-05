{{ config(materialized=env_var("DBT_MAT_TABLE"),tags=["sales","budget"]) }}
--change this source materialization to table. Since this is being used in the view v_sls_wtx_budget_pcos_projections
--as recursive cte. Keeping it as view was throwing SQL execution internal error: Processing aborted due to error 300010:423728544; incident 9425260.

with
d365_source as (

    select *
    from {{ source("D365S", "bomcalctrans") }}
    where _fivetran_deleted = 'FALSE'
    and upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}

),

renamed as (


    select
        'D365S' as source,
        costgroupid as costgroupid,
        level as level_,
        qty as qty,
        costprice as costprice,
        costmarkup as costmarkup,
        salesprice as salesprice,
        salesmarkup as salesmarkup,
        cast(transdate as TIMESTAMP_NTZ) as transdate,
        cast(linenum as DECIMAL(18, 16)) as linenum,
        resource as resource_,
        unitid as unitid,
        oprid as oprid,
        inventdimstr as inventdimstr,
        consumptionvariable as consumptionvariable,
        consumptionconstant as consumptionconstant,
        bom as bom,
        oprnum as oprnum,
        calctype as calctype,
        costpriceunit as costpriceunit,
        costpriceqty as costpriceqty,
        salespriceqty as salespriceqty,
        costmarkupqty as costmarkupqty,
        salesmarkupqty as salesmarkupqty,
        pricecalcid as pricecalcid,
        numofseries as numofseries,
        oprnumnext as oprnumnext,
        oprpriority as oprpriority,
        consumptioninvent as consumptioninvent,
        inventdimid as inventdimid,
        null as vendid,
        consumptype as consumptype,
        salespriceunit as salespriceunit,
        netweightqty as netweightqty,
        null as infolog,
        salespricemodelused as salespricemodelused,
        pricediscqty as pricediscqty,
        costpricemodelused as costpricemodelused,
        calcgroupid as calcgroupid,
        null as costpricefallbackversion,
        null as salespricefallbackversion,
        routelevel as routelevel,
        costpriceqtyseccur_ru as costpriceqtyseccur_ru,
        costmarkupqtyseccur_ru as costmarkupqtyseccur_ru,
        costpriceseccur_ru as costpriceseccur_ru,
        costmarkupseccur_ru as costmarkupseccur_ru,
        null as consistofprice,
        parentbomcalctrans as parentbomcalctrans,
        costcalculationmethod as costcalculationmethod,
        cast(createddatetime as TIMESTAMP_NTZ) as createddatetime,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source
    
)

select * from renamed