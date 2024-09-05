with
    d365_source as (
        select *
        from {{ source("D365", "invent_table_module") }} where _FIVETRAN_DELETED='FALSE' and upper(trim(data_area_id)) in {{env_var("DBT_D365_COMPANY_FILTER")}} 

    ),

    renamed as (

        select
            'D365' as source,
            item_id as itemid,
            module_type as moduletype,
            unit_id as unitid,
            price as price,
            price_unit as priceunit,
            markup as markup,
            null as linedisc,
            null as multilinedisc,
            end_disc as enddisc,
            tax_item_group_id as taxitemgroupid,
            markup_group_id as markupgroupid,
            price_date as pricedate,
            price_qty as priceqty,
            allocate_markup as allocatemarkup,
            over_delivery_pct as overdeliverypct,
            under_delivery_pct as underdeliverypct,
            null as suppitemgroupid,
            inter_company_blocked as intercompanyblocked,
            tax_withhold_item_group_heading_th as taxwithholditemgroupheading_th,
            tax_withhold_calculate_th as taxwithholdcalculate_th,
            maximum_retail_price_in as maximumretailprice_in,
            price_sec_cur_ru as priceseccur_ru,
            markup_sec_cur_ru as markupseccur_ru,
            pdspricing_precision as pdspricingprecision,
            tax_gstrelief_category_my as taxgstreliefcategory_my,
            modifieddatetime as modifieddatetime,
            null as del_modifiedtime,
            modifiedby as modifiedby,
            createddatetime as createddatetime,
            null as del_createdtime,
            createdby as createdby,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid

        from d365_source

    )

select *
from renamed 
