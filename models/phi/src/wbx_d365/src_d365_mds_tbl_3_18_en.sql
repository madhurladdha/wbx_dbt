

with source as (

    select * from {{ source('WEETABIX', 'MDS_tbl_3_18_EN') }}

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
        uda_18_478,
        uda_18_479,
        uda_18_482,
        uda_18_483,
        uda_18_484,
        uda_18_567,
        uda_18_568,
        uda_18_1559,
        uda_18_1735,
        uda_18_1736

    from source

)

select * from renamed
