

with source as (

    select * from {{ source('WEETABIX', 'whsinventtable') }}

),

renamed as (

    select
        uomseqgroupid,
        maxpickqty,
        itemid,
        rfdescription1,
        rfdescription2,
        packsizecateogryid,
        filtercode,
        filtercode2_,
        filtercode3_,
        filtercode4_,
        filtergroup,
        filtergroup2_,
        filterchanged,
        prodqty,
        physdimid,
        packageclassid,
        pickwcneg,
        modifieddatetime,
        modifiedby,
        dataareaid,
        recversion,
        partition,
        recid,
        allowmaterialoverpick

    from source

)

select * from renamed
