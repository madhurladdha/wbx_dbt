
with

d365_source as (
    select *
    from {{ source("D365S", "dimensionattribute") }}
    where _fivetran_deleted = 'FALSE'


),

renamed as (

    select
        'D365S' as source,
        name as name,
        keyattribute as keyattribute,
        valueattribute as valueattribute,
        nameattribute as nameattribute,
        backingentitytype as backingentitytype,
        reportcolumnname as reportcolumnname,
        hashkey as hashkey,
        type as type,
        viewname as viewname,
        backingentitytablename as backingentitytablename,
        backingentitykeyfieldname as backingentitykeyfieldname,
        backingentityvaluefieldname as backingentityvaluefieldname,
        backingentitytableid as backingentitytableid,
        backingentitykeyfieldid as backingentitykeyfieldid,
        backingentityvaluefieldid as backingentityvaluefieldid,
        isbalancing_psn as isbalancing_psn,
        balancingdimension_psn as balancingdimension_psn,
        null as docudatasourcequeryname,
        translationkeyfieldid as translationkeyfieldid,
        translationkeyfieldname as translationkeyfieldname,
        translationlanguageidfieldid as translationlanguageidfieldid,
        translationlanguageidfieldname as translationlanguageidfieldname,
        translationnamefieldid as translationnamefieldid,
        translationnamefieldname as translationnamefieldname,
        translationtableid as translationtableid,
        translationtablename as translationtablename,
        translationviewid as translationviewid,
        translationviewkeyfieldid as translationviewkeyfieldid,
        translationviewkeyfieldname as translationviewkeyfieldname,
        translationviewlanguageidfieldid as translationviewlangidfieldid,
        translationviewlanguageidfieldname
            as translationviewlangidfieldname,
        translationviewname as translationviewname,
        translationviewnamefieldid as translationviewnamefieldid,
        translationviewnamefieldname as translationviewnamefieldname,
        translationviewsystemlanguageidfieldid
            as transviewsystemlangidfieldid,
        translationviewsystemlanguageidfieldname
            as transviewsystemlangidfieldname,
        translationviewtranslatednamefieldid
            as transviewtranslatednamefieldid,
        translationviewtranslatednamefieldname
            as transviewtransnamefieldname,
        translationviewvaluefieldid as translationviewvaluefieldid,
        translationviewvaluefieldname as translationviewvaluefieldname,
        usetranslationnamemethod as usetranslationnamemethod,
        modifiedby as modifiedby,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed