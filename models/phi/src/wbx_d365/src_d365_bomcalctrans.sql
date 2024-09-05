with
    d365_source as (

        select * from {{ source("D365", "bomcalc_trans") }} where _FIVETRAN_DELETED='FALSE'
    ),

    renamed as (

        
        select
            'D365' as source,
            cost_group_id as costgroupid,
            level_ as level_,
            qty as qty,
            cost_price as costprice,
            cost_markup as costmarkup,
            sales_price as salesprice,
            sales_markup as salesmarkup,
            trans_date as transdate,
             cast(line_num as decimal(18,16))  linenum,
            resource_ as resource_,
            unit_id as unitid,
            opr_id as oprid,
            invent_dim_str as inventdimstr,
            consumption_variable as consumptionvariable,
            consumption_constant as consumptionconstant,
            bom as bom,
            opr_num as oprnum,
            calc_type as calctype,
            cost_price_unit as costpriceunit,
            cost_price_qty as costpriceqty,
            sales_price_qty as salespriceqty,
            cost_markup_qty as costmarkupqty,
            sales_markup_qty as salesmarkupqty,
            price_calc_id as pricecalcid,
            num_of_series as numofseries,
            opr_num_next as oprnumnext,
            opr_priority as oprpriority,
            consumption_invent as consumptioninvent,
            invent_dim_id as inventdimid,
            null as vendid,
            consump_type as consumptype,
            sales_price_unit as salespriceunit,
            net_weight_qty as netweightqty,
            null as infolog,
            sales_price_model_used as salespricemodelused,
            price_disc_qty as pricediscqty,
            cost_price_model_used as costpricemodelused,
            calc_group_id as calcgroupid,
            null as costpricefallbackversion,
            null as salespricefallbackversion,
            route_level as routelevel,
            cost_price_qty_sec_cur_ru as costpriceqtyseccur_ru,
            cost_markup_qty_sec_cur_ru as costmarkupqtyseccur_ru,
            cost_price_sec_cur_ru as costpriceseccur_ru,
            cost_markup_sec_cur_ru as costmarkupseccur_ru,
            null as consistofprice,
            parent_bomcalc_trans  as parentbomcalctrans,
            cost_calculation_method as costcalculationmethod,
            createddatetime as createddatetime,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid
        from d365_source where upper(dataareaid) in {{env_var("DBT_D365_COMPANY_FILTER")}}
    )

select * from renamed 