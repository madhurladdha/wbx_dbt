

with source as (

    select * from {{ source('WEETABIX', 'MDS_tbl_9_83_EN') }}

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
        UDA_83_2224,
        UDA_83_2225
   

    from source

)

select * from renamed
