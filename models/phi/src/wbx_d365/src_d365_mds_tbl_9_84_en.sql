

with source as (

    select * from {{ source('WEETABIX', 'MDS_tbl_9_84_EN') }}

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
        uda_84_2242,
        uda_84_2243
      

    from source

)

select * from renamed
