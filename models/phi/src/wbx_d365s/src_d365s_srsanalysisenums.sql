with d365_source as (
    select *
    from {{ source("D365S", "srsanalysisenums") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365S' as source,
        enumitemvalue as enumitemvalue,
        null as enumitemlabel,
        null as languageid,
        enumname as enumname,
        enumitemname as enumitemname,
        recversion as recversion,
        recid as recid
    from d365_source

)

select *
from renamed

