
with source as (

    select * from {{ source('WEETABIX','SRPT_Manufacturing Groups') }}

),

renamed as (

    select
MANUGRPNO,
MANUGRPDESC,
MANUGRPSORT
from source

)

select * from renamed