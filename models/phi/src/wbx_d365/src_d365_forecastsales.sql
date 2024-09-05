with
    d365_source as (
        select *
        from {{ source("D365", "forecast_sales") }} where _FIVETRAN_DELETED='FALSE' and upper(data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}
    ),

    renamed as (

       
        select
            'D365' as source,
            item_id as itemid,
            start_date as startdate,
            end_date as enddate,
            freq_code as freqcode,
            active as active,
            invent_qty as inventqty,
            sales_price as salesprice,
            disc_percent as discpercent,
            comment_ as comment_,
            cust_group_id as custgroupid,
            item_group_id as itemgroupid,
            cust_account_id as custaccountid,
            null as keyid,
            currency as currency,
            expand_id as expandid,
            report as report,
            sales_qty as salesqty,
            sales_unit_id as salesunitid,
            sales_markup as salesmarkup,
            disc_amount as discamount,
            price_unit as priceunit,
            cost_price as costprice,
            tax_item_group_id as taxitemgroupid,
            cov as cov,
            cov_status as covstatus,
            item_allocate_id as itemallocateid,
            tax_group_id as taxgroupid,
            freq as freq,
            amount as amount,
            model_id as modelid,
            allocate_method as allocatemethod,
            null as itembomid,
            null as itemrouteid,
            default_dimension as defaultdimension,
            null as projid,
            null as projcategoryid,
            null as projlinepropertyid,
            invent_dim_id as inventdimid,
            null as projtransid,
            proj_forecast_sales_paym_date as projforecastsalespaymdate,
            proj_forecast_cost_paym_date as projforecastcostpaymdate,
            proj_forecast_invoice_date as projforecastinvoicedate,
            proj_forecast_elimination_date as projforecasteliminationdate,
            null as activitynumber,
            proj_forecast_budget_type as projforecastbudgettype,
            proj_funding_source as projfundingsource,
            psaref_purch_line as psarefpurchline,
            pds_cwqty as pdscwqty,
            null as pdscwunitid,
            modifiedby as modifiedby,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid,
            null as wbxforecastdifference,
            null as wbxforecastallocationpct
        from d365_source 

    )

select *
from renamed

