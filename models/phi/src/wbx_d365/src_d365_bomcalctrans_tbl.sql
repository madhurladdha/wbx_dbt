{{ config(materialized=env_var("DBT_MAT_TABLE"),tags=["sales","budget"]) }}
--change this source materialization to table. Since this is being used in the view v_sls_wtx_budget_pcos_projections
--as recursive cte. Keeping it as view was throwing SQL execution internal error: Processing aborted due to error 300010:423728544; incident 9425260.
with source as (

    select * from {{ source('D365', 'bomcalc_trans') }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        cost_group_id as costgroupid,
        level_,
        qty,
        cost_price as costprice,
        cost_markup as costmarkup,
        sales_price as salesprice,
        sales_markup as salesmarkup,
        trans_date as transdate,
        line_num as linenum,
        resource_,
        unit_id as unitid,
        opr_id as oprid,
        invent_dim_str as inventdimstr,
        consumption_variable as consumptionvariable,
        consumption_constant as consumptionconstant,
        bom,
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
        -- cost_price_fallback_version as costpricefallbackversion,
        -- sales_price_fallback_version as salespricefallbackversion,
        route_level as routelevel,
        cost_price_qty_sec_cur_ru as costpriceqtyseccur_ru,
        cost_markup_qty_sec_cur_ru as costmarkupqtyseccur_ru,
        cost_price_sec_cur_ru as costpriceseccur_ru,
        cost_markup_sec_cur_ru as costmarkupseccur_ru,
        consist_of_price as consistofprice,
        parent_bomcalc_trans as parentbomcalctrans,
        cost_calculation_method as costcalculationmethod,
        createddatetime,
        upper(data_area_id) as dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
