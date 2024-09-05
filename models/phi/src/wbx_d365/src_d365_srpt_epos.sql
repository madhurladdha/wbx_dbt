with
    source as (select * from {{ source("WEETABIX", "SRPT_EPOS") }}),

    renamed as (select tratypcde, comcde5d, cyrwk, eposcases from source)

select *
from renamed
