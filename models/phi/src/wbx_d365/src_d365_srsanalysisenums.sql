with d365_source as (
    select *
    from {{ source("D365", "srsanalysis_enums") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365' as source,
        enum_item_value as enumitemvalue,
        null as enumitemlabel,
        null as languageid,
        enum_name as enumname,
        enum_item_name as enumitemname,
        recversion as recversion,
        recid as recid
    from d365_source

)

select *
from renamed

