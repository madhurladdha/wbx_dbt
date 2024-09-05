

with source as (

    select * from {{ source('WEETABIX', 'MDS_tbl_3_12_EN') }}

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
        uda_12_332,
        uda_12_333,
        uda_12_334,
        uda_12_335,
        uda_12_336,
        uda_12_337,
        uda_12_338

    from source

)

select * from renamed
