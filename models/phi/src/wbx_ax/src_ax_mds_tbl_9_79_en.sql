

with source as (

    select * from {{ source('WEETABIX', 'MDS_tbl_9_79_EN') }}

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
UDA_79_2148,
UDA_79_2149
    from source

)

select * from renamed
