

with source as (

    select * from {{ source('WEETABIX', 'MDS_tbl_3_59_EN') }}

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
        uda_59_1599,
        uda_59_1600,
        uda_59_1601,
        uda_59_1602,
        uda_59_1603,
        uda_59_1604,
        uda_59_1605,
        uda_59_1606

    from source

)

select * from renamed
