with d365_source as (

        select * from {{ source("D365", "bomcalc_table") }} where _FIVETRAN_DELETED='FALSE' 
        ),

    renamed as (


        select
            'D365' as source,
            item_id as itemid,
            trans_date as transdate,
            qty as qty,
            cost_price as costprice,
            cost_markup as costmarkup,
            sales_price as salesprice,
            sales_markup as salesmarkup,
            unit_id as unitid,
            profit_set as profitset,
            bomid as bomid,
            route_id as routeid,
            price_calc_id as pricecalcid,
            invent_dim_id as inventdimid,
            net_weight as netweight,
            lean_production_flow_reference as leanproductionflowreference,
            bomcalc_type as bomcalctype,
            cost_price_sec_cur_ru as costpriceseccur_ru,
            cost_markup_sec_cur_ru as costmarkupseccur_ru,
            cost_calculation_method as costcalculationmethod,
            pmf_bom_version  as pmfbomversion,
            null as pmfparentcalcid,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid
        from d365_source where upper(dataareaid) in {{env_var("DBT_D365_COMPANY_FILTER")}} 

    )

select * from renamed 