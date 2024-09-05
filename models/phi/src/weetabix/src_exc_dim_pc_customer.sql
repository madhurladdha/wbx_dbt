with source as (

    select * from {{ source('WEETABIX', 'EXC_Dim_PC_Customer') }}

),

renamed as (

    select
IDX,
CODE,
NAME,
CURRENCY_IDX,
INAPPLICATION,
ISDIRECTCUSTOMER,
ISNONCUSTOMERPAYEE,
ISINDIRECTCUSTOMER,
DATE_INSERTED,
DATE_UPDATED

    from source

)

select * from renamed