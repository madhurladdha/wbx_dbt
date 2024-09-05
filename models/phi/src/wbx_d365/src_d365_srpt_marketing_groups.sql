

with source as (

    select * from {{ source('WEETABIX','SRPT_Marketing Groups') }}

),

renamed as (

select
 MKTGRPNO,
MKTGRPDESC,
MKTGRPSQN
from source

)

select * from renamed