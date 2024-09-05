with
    d365_source as (
        select *
        from {{ source("D365", "invent_trans_origin_prod_table") }}  where _FIVETRAN_DELETED='FALSE' and upper(prod_order_data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}
    ),

    renamed as (

        select
            'D365' as source,
            prod_order_id as prodorderid,
            invent_trans_origin as inventtransorigin,
            upper(prod_order_data_area_id) as prodorderdataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid

        from d365_source

    )

select * from renamed 