

with source as (

    select * from {{ source('WEETABIX', 'wmspickingroute') }}

),

renamed as (

    select
        pickingrouteid,
        shipmentid,
        expeditionstatus,
        customer,
        transtype,
        transrefid,
        handlingtype,
        inventlocationid,
        volume,
        optimizedpicking,
        expectedexpeditiontime,
        pickingareaid,
        priority,
        currentpickpalletid,
        startdatetime,
        startdatetimetzid,
        enddatetime,
        enddatetimetzid,
        pallettagging,
        activationdatetime,
        activationdatetimetzid,
        intercompanyposted,
        shipmenttype,
        dlvmodeid,
        dlvtermid,
        dlvdate,
        deliveryname,
        operatorworker,
        autodecreaseqty,
        printmgmtsiteid,
        parmid,
        deliverypostaladdress,
        mcrpickingwaveref,
        mcrpackingboxname,
        modifieddatetime,
        modifiedby,
        modifiedtransactionid,
        createddatetime,
        createdby,
        createdtransactionid,
        upper(dataareaid) as dataareaid,
        recversion,
        partition,
        recid,
        wbxpickingdatetime,
        wbxpickingdatetimetzid,
        bisediprocess

    from source

)

select * from renamed
