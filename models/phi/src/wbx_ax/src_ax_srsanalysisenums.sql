

with source as (

    select * from {{ source('WEETABIX', 'srsanalysisenums') }}

),

renamed as (

    select
        enumitemvalue,
        enumitemlabel,
        languageid,
        enumname,
        enumitemname,
        recversion,
        recid

    from source

)

select * from renamed
