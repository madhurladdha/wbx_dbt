with

    d365_source as (
        select *
        from {{ source("D365", "cust_confirm_trans") }}
        where trim(upper(data_area_id)) in {{env_var("DBT_D365_COMPANY_FILTER")}} and _FIVETRAN_DELETED='FALSE' 

    ),
renamed as (

    select
        'D365' as source,
        sales_id as salesid,
        confirm_id as confirmid,
        confirm_date as confirmdate,
        line_num as linenum,
        sales_category as salescategory,
        item_id as itemid,
        external_item_id as externalitemid,
        name as name,
        currency_code as currencycode,
        price_unit as priceunit,
        sales_unit as salesunit,
        qty as qty,
        sales_price as salesprice,
        sales_markup as salesmarkup,
        disc_percent as discpercent,
        disc_amount as discamount,
        line_amount as lineamount,
        default_dimension as defaultdimension,
        dlv_date as dlvdate,
        invent_trans_id as inventtransid,
        tax_amount as taxamount,
        null as taxwritecode,
        multi_ln_disc as multilndisc,
        multi_ln_percent as multilnpercent,
        line_disc as linedisc,
        line_percent as linepercent,
        tax_group as taxgroup,
        tax_item_group as taxitemgroup,
        null as salesgroup,
        orig_sales_id as origsalesid,
        line_header as lineheader,
        invent_dim_id as inventdimid,
        invent_qty as inventqty,
        line_amount_tax as lineamounttax,
        stocked_product as stockedproduct,
        dlv_term as dlvterm,
        pds_cwqty as pdscwqty,
        upper(data_area_id) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed
