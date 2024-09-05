with
    d365_source as (
        select *
        from {{ source("D365", "invent_item_price") }} where _FIVETRAN_DELETED='FALSE' and upper(data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}
    ),

    renamed as (
        select
            'D365' as source,
            item_id as itemid,
            cast(version_id as varchar(255)) as versionid,
            price_type as pricetype,
            invent_dim_id as inventdimid,
            markup as markup,
            price_unit as priceunit,
            cast(price as number(32,16) ) as price,
            price_calc_id as pricecalcid,
            unit_id as unitid,
            price_allocate_markup as priceallocatemarkup,
            price_qty as priceqty,
            std_cost_trans_date as stdcosttransdate,
            std_cost_voucher as stdcostvoucher,
            costing_type as costingtype,
            activation_date as activationdate,
            price_sec_cur_ru as priceseccur_ru,
            markup_sec_cur_ru as markupseccur_ru,
            modifieddatetime as modifieddatetime,
            createddatetime as createddatetime,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid
        from d365_source 

    )

select *
from renamed
