with d365_source as (
        select *
        from {{ source("D365S", "whsinventtable") }} where _FIVETRAN_DELETED='FALSE' and upper(dataareaid) in {{env_var("DBT_D365_COMPANY_FILTER")}}
    ),

    renamed as (
        select
            'D365S' as source,
            uomseqgroupid as uomseqgroupid,
            maxpickqty as maxpickqty,
            itemid as itemid,
            null as rfdescription1,
            null as rfdescription2,
            null as packsizecateogryid,
            --NOTE filtercode and filtercode2_ are set to null
            null as filtercode,
            null as filtercode2_,
            null as filtercode3_,
            null as filtercode4_,
            null as filtergroup,
            null as filtergroup2_,
            filterchanged as filterchanged,
            prodqty as prodqty,
            null as physdimid,
            null as packageclassid,
            pickwcneg as pickwcneg,
            cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
            modifiedby as modifiedby,
            upper(dataareaid) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid,
            allowmaterialoverpick as allowmaterialoverpick
        from d365_source

    )

select *
from renamed
qualify row_number() over(partition by itemid,dataareaid order by source desc)=1