

with source as (

    select * from {{ source('WEETABIX', 'MDS_tbl_9_77_EN') }}

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
UDA_77_2112,
UDA_77_2113
    from source

)

select * from renamed
