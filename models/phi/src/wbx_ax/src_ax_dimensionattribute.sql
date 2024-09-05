

with source as (

    select * from {{ source('WEETABIX', 'dimensionattribute') }}

),

renamed as (

    select
        name,
        keyattribute,
        valueattribute,
        nameattribute,
        backingentitytype,
        reportcolumnname,
        hashkey,
        type,
        viewname,
        backingentitytablename,
        backingentitykeyfieldname,
        backingentityvaluefieldname,
        backingentitytableid,
        backingentitykeyfieldid,
        backingentityvaluefieldid,
        isbalancing_psn,
        balancingdimension_psn,
        docudatasourcequeryname,
        translationkeyfieldid,
        translationkeyfieldname,
        translationlanguageidfieldid,
        translationlanguageidfieldname,
        translationnamefieldid,
        translationnamefieldname,
        translationtableid,
        translationtablename,
        translationviewid,
        translationviewkeyfieldid,
        translationviewkeyfieldname,
        translationviewlangidfieldid,
        translationviewlangidfieldname,
        translationviewname,
        translationviewnamefieldid,
        translationviewnamefieldname,
        transviewsystemlangidfieldid,
        transviewsystemlangidfieldname,
        transviewtranslatednamefieldid,
        transviewtransnamefieldname,
        translationviewvaluefieldid,
        translationviewvaluefieldname,
        usetranslationnamemethod,
        modifiedby,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
