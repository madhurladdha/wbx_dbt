with d365_source as (
        select *
        from {{ source("D365", "whsinvent_table") }} where _FIVETRAN_DELETED='FALSE' and upper(data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}
    ),

    renamed as (
        select
            'D365' as source,
            uomseq_group_id as uomseqgroupid,
            max_pick_qty as maxpickqty,
            item_id as itemid,
            null as rfdescription1,
            null as rfdescription2,
            null as packsizecateogryid,
            filter_code as filtercode,
            filtercode_2_ as filtercode2_,
            null as filtercode3_,
            null as filtercode4_,
            null as filtergroup,
            null as filtergroup2_,
            filter_changed as filterchanged,
            prod_qty as prodqty,
            null as physdimid,
            null as packageclassid,
            pick_wcneg as pickwcneg,
            modifieddatetime as modifieddatetime,
            modifiedby as modifiedby,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid,
            allow_material_over_pick as allowmaterialoverpick
        from d365_source

    )

select *
from renamed
qualify row_number() over(partition by itemid,dataareaid order by source desc)=1