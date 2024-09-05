

with source as (

    select * from {{ source('WEETABIX', 'MDS_tbl_9_78_EN') }}

),

renamed as (

    select
               VERSION_ID,
ID,
STATUS_ID,
VALIDATIONSTATUS_ID,
NAME,
CODE,
CHANGETRACKINGMASK,
ENTERDTM,
ENTERUSERID,
ENTERVERSIONID,
LASTCHGDTM,
LASTCHGUSERID,
LASTCHGVERSIONID,
LASTCHGTS,
ASOF_ID,
MUID,
UDA_78_2130,
UDA_78_2131

    from source

)

select * from renamed
