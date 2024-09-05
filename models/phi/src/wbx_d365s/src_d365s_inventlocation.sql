with
d365_source as (
    select *
    from {{ source("D365S", "inventlocation") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(trim(dataareaid)) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (

    select
        'D365S' as source,
        inventlocationid as inventlocationid,
        name as name,
        manual as manual,
        null as emptypalletlocation,
        maxpickingroutevolume as maxpickingroutevolume,
        pickinglinetime as pickinglinetime,
        maxpickingroutetime as maxpickingroutetime,
        wmslocationiddefaultreceipt as wmslocationiddefaultreceipt,
        wmslocationiddefaultissue as wmslocationiddefaultissue,
        null as inventlocationidreqmain,
        reqrefill as reqrefill,
        inventlocationtype as inventlocationtype,
        null as inventlocationidquarantine,
        inventlocationlevel as inventlocationlevel,
        null as reqcalendarid,
        wmsaislenameactive as wmsaislenameactive,
        wmsracknameactive as wmsracknameactive,
        null as wmsrackformat,
        wmslevelnameactive as wmslevelnameactive,
        null as wmslevelformat,
        wmspositionnameactive as wmspositionnameactive,
        null as wmspositionformat,
        usewmsorders as usewmsorders,
        inventlocationidtransit as inventlocationidtransit,
        null as vendaccount,
        null as branchnumber,
        inventsiteid as inventsiteid,
        inventcountinggroup_br as inventcountinggroup_br,
        null as custaccount_br,
        inventprofiletype_ru as inventprofiletype_ru,
        null as inventprofileid_ru,
        null as inventlocidgoodsinroute_ru,
        null as wmslocidgoodsinroute_ru,
        null as activitytype_ru,
        null as numbersequencegroup_ru,
        retailweightex_1 as retailweightex1,
        fshstore as fshstore,
        null as rbodefaultwmspalletid,
        null as rbodefaultwmslocationid,
        null as rbodefaultinventprofileid_ru,
        allowlaborstandards as allowlaborstandards,
        allowmarkingreservationremoval as allowmarkingreservationremoval,
        consolidateshipatrtw as consolidateshipatrtw,
        null as custaccount_hu,
        cyclecountallowpalletmove as cyclecountallowpalletmove,
        decrementloadline as decrementloadline,
        null as dfltkanbanfinishedgoodsloc,
        defaultproductionfinishgoodslocation as dfltproductionfinishgoodsloc,
        null as defaultshipmaintenanceloc,
        defaultstatusid as defaultstatusid,
        printbolbeforeshipconfirm as printbolbeforeshipconfirm,
        prodreserveonlywhse as prodreserveonlywhse,
        null as removeinventblockonstatchange,
        reserveatloadpost as reserveatloadpost,
        retailinventnegfinancial as retailinventnegfinancial,
        retailinventnegphysical as retailinventnegphysical,
        null as retailwmslociddefaultreturn,
        null as retailwmspalletiddfltreturn,
        uniquecheckdigits as uniquecheckdigits,
        null as vendaccountcustom_ru,
        null as warehouseautoreleaseres,
        whsenabled as whsenabled,
        null as defaultproductioninputloc,
        null as defaultreturncreditonlyloc,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        null as del_modifiedtime,
        modifiedby as modifiedby,
        cast(createddatetime as TIMESTAMP_NTZ) as createddatetime,
        null as del_createdtime,
        createdby as createdby,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid,
        null as wbxtooutbound,
        null as wbxproposedefaultvariant,
        whsrawmaterialpolicy as whsrawmaterialpolicy,
        null as wbxinventlocationid,
        null as wbxinventsiteid,
        null as wbxwmstransferenabled,
        null as biseancodeid,
        null as defaultcontainertypecode,
        null as defaultshipdirectlocation,
        null as includedemandforecast

    from d365_source

)

select *
from renamed
