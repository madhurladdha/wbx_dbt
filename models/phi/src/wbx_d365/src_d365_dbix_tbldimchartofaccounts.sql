

with source as (

    select * from {{ source('WEETABIX', 'DBIX_tblDimChartOfAccounts') }}

),

renamed as (

    select
KCOA_LINE,
DCA_COA_LINESEQUENCE,
DCA_COA_LINEPARENT,
DCA_ACCOUNTDESCRIPTION,
DCA_UNARYOPERATOR,
DCA_TYPE,
DCA_CONTRA,
DCA_CALCULATION,
DCA_INFO,
DCA_ACCOUNTTYPE,
DCA_REPORTLEVEL
from source

)

select * from renamed
