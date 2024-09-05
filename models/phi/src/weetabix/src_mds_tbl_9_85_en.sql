

with source as (

    select * from {{ source('WEETABIX', 'MDS_tbl_9_85_EN') }}

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
       uda_85_2260,
       uda_85_2261

    from source

)

select * from renamed
