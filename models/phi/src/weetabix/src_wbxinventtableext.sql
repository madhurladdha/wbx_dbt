

with source as (

    select * from {{ source('WEETABIX', 'wbxinventtableext') }}

),

renamed as (

    select
        itemid,
        reasoncodemandatory,
        reasondescmandatory,
        upper(dataareaid) as dataareaid,
        recversion,
        partition,
        recid,
        palletconfiguration,
        pallettype,
        consumerunits,
        inventsizeid,
        totalsoqtydisc,
        additionaldisc,
        palletqty,
        palletqtyperlayer,
        wbxsafetystockcoverage

    from source

)

select * from renamed
