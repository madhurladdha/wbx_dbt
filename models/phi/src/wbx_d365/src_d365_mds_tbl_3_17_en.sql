

with source as (

    select * from {{ source('WEETABIX', 'MDS_tbl_3_17_EN') }}

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
        uda_17_455,
        uda_17_456,
        uda_17_457,
        uda_17_458,
        uda_17_459,
        uda_17_460,
        uda_17_461

    from source

)

select * from renamed
