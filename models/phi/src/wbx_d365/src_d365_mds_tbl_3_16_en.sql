

with source as (

    select * from {{ source('WEETABIX', 'MDS_tbl_3_16_EN') }}

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
        uda_16_432,
        uda_16_433,
        uda_16_436,
        uda_16_437,
        uda_16_438,
        uda_16_565,
        uda_16_566,
        uda_16_1632

    from source

)

select * from renamed
