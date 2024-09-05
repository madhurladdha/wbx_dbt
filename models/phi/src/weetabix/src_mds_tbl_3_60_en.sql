

with source as (

    select * from {{ source('WEETABIX', 'MDS_tbl_3_60_EN') }}

),

renamed as (

    select
        version_id,
        id,
        status_id,
        validationstatus_id,
        name,
        code,
        changetrackingmask,
        enterdtm,
        enteruserid,
        enterversionid,
        lastchgdtm,
        lastchguserid,
        lastchgversionid,
        lastchgts,
        asof_id,
        muid,
        uda_60_1624,
        uda_60_1625,
        uda_60_1626,
        uda_60_1627,
        uda_60_1628,
        uda_60_1629,
        uda_60_1630,
        uda_60_1631

    from source

)

select * from renamed
