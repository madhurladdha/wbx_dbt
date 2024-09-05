

with source as (

    select * from {{ source('WEETABIX', 'MDS_tbl_9_86_EN') }}

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
        uda_86_2278,
        uda_86_2279

    from source

)

select * from renamed
