{{
    config(
        materialized=env_var("DBT_RZ_DS_MAT"),
        snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
        tags=["redzone", "OEE", "v_productunitconversion", "v_losses"],
    )
}}

with
    conv as (select * from {{ ref("v_rz_productunitconversion_norm") }}),

    loss as (select * from {{ ref("src_rz_v_losses") }}),

    cp as (
        select
            to_boolean(ifnull(normalized_value, false)) as choke_point,
            source_value,
            source_site
        from {{ ref("src_rz_wbx_normalization") }}
        where reference_field = 'OEE_INCLUDE'
    ),

    losses as (
        select loss.*, ifnull(choke_point, false) as choke_point
        from loss
        left join
            cp
            on loss."locationName" = cp.source_value
            and loss."siteName" = cp.source_site
    )

select
    loss."siteId",
    loss."problemID",
    loss."timeZoneId",
    loss."dateYear",
    loss."quarter",
    loss."quarterName",
    loss."monthNumber",
    loss."monthName",
    loss."week",
    loss."dayName",
    loss."dayOfWeekNumber",
    loss."dayOfWeekNumberIso",
    loss."hourOfDay",
    loss."dateTimeNearestHour",
    loss."startTime",
    loss."endTime",
    loss."secondsLost",
    loss."minutesLost",
    loss."hoursLost",
    loss."unitsLost",
    loss."potentialUnitsLost",
    loss."unitOfMeasureUUID",
    loss."unitOfMeasureName",
    loss."runUnitsLost",
    loss."runPotentialUnitsLost",
    loss."runUnitOfMeasureUUID",
    loss."runUnitOfMeasureName",
    loss."createdDate",
    loss."updatedDate",
    loss."locationUUID",
    loss."locationName",
    loss."problemTypeAssetTypeUUID",
    loss."problemTypeAssetTypeName",
    loss."problemTypeAssetUUID",
    loss."problemTypeAssetName",
    loss."areaUUID",
    loss."areaName",
    loss."siteUUID",
    loss."siteName",
    loss."problemTypeUUID",
    loss."problemTypeName",
    loss."problemGroupUUID",
    loss."problemGroupName",
    loss."problemReportGroup",
    loss."problemCategory",
    loss."problemPlanned",
    loss."problemCountRelationship",
    loss."clockType",
    loss."scheduledClockEndTime",
    loss."problemTypeAutoAssigned",
    loss."runUUID",
    loss."runName",
    loss."productTypeUUID",
    loss."customReference",
    loss."runId",
    loss."locked",
    loss."hasQualityChecks",
    loss."signatureOnHold",
    loss."runSignatureUsername",
    loss."runSignatureLastName",
    loss."runSignatureFirstName",
    loss."runSignatureTime",
    loss."runSignatureType",
    loss."productTypeName",
    loss."productTypeSKU",
    loss."productTypeLabelWeight",
    loss."manufacturingTypeUUID",
    loss."manufacturingTypeName",
    loss."shiftUUID",
    loss."shiftName",
    loss."productExternalId",
    loss."productUUID",
    loss."stationUUID",
    loss."stationName",
    loss."stepUUID",
    loss."stepName",
    loss."CHOKE_POINT",
    conv."TOUOMNAME_NORM",
    conv."FROMUOMNAME_NORM",
    conv."TOVALUE_NORM",
    case
        when loss."unitOfMeasureName" = 'LB'
        then loss."unitsLost"
        when loss."unitOfMeasureName" = 'LBS'
        then loss."unitsLost"
        when loss."unitOfMeasureName" = 'Kgs'
        then (loss."unitsLost" * 2.20462)
        when loss."runUnitOfMeasureName" = 'LB'
        then loss."runUnitsLost"
        when loss."runUnitOfMeasureName" = 'LBS'
        then loss."runUnitsLost"
        when loss."runUnitOfMeasureName" = 'Kgs'
        then (loss."runUnitsLost" * 2.20462)
        when loss."runUnitOfMeasureName" is not null and conv.tovalue_norm is not null
        then
            (
                case
                    conv.touomname_norm
                    when 'Kgs'
                    then ((loss."runUnitsLost" * tovalue_norm) * 2.20462)
                    else (loss."runUnitsLost" * tovalue_norm)
                end
            )
        else loss."unitsLost"
    end as lbs_unitslost,
    case
        when loss."unitOfMeasureName" = 'LB'
        then (loss."unitsLost" * 0.45359237)
        when loss."unitOfMeasureName" = 'LBS'
        then (loss."unitsLost" * 0.45359237)
        when loss."unitOfMeasureName" = 'Kgs'
        then loss."unitsLost"
        when loss."runUnitOfMeasureName" = 'LB'
        then (loss."runUnitsLost" * 0.45359237)
        when loss."runUnitOfMeasureName" = 'LBS'
        then (loss."runUnitsLost" * 0.45359237)
        when loss."runUnitOfMeasureName" = 'Kgs'
        then loss."runUnitsLost"
        when loss."runUnitOfMeasureName" is not null and conv.tovalue_norm is not null
        then
            (
                case
                    conv.touomname_norm
                    when 'Kgs'
                    then (loss."runUnitsLost" * tovalue_norm)
                    else ((loss."runUnitsLost" * tovalue_norm) * 0.45359237)
                end
            )
        else loss."unitsLost"
    end as kgs_unitslost,

    case
        when loss."unitOfMeasureName" = 'LB'
        then loss."potentialUnitsLost"
        when loss."unitOfMeasureName" = 'LBS'
        then loss."potentialUnitsLost"
        when loss."unitOfMeasureName" = 'Kgs'
        then (loss."potentialUnitsLost" * 2.20462)
        when loss."runUnitOfMeasureName" = 'LB'
        then loss."runPotentialUnitsLost"
        when loss."runUnitOfMeasureName" = 'LBS'
        then loss."runPotentialUnitsLost"
        when loss."runUnitOfMeasureName" = 'Kgs'
        then (loss."runPotentialUnitsLost" * 2.20462)
        when loss."runUnitOfMeasureName" is not null and conv.tovalue_norm is not null
        then
            (
                case
                    conv.touomname_norm
                    when 'Kgs'
                    then ((loss."runPotentialUnitsLost" * tovalue_norm) * 2.20462)
                    else (loss."runPotentialUnitsLost" * tovalue_norm)
                end
            )
        else loss."potentialUnitsLost"
    end as lbs_potentialunitslost,
    case
        when loss."unitOfMeasureName" = 'LB'
        then (loss."potentialUnitsLost" * 0.45359237)
        when loss."unitOfMeasureName" = 'LBS'
        then (loss."potentialUnitsLost" * 0.45359237)
        when loss."unitOfMeasureName" = 'Kgs'
        then loss."potentialUnitsLost"
        when loss."runUnitOfMeasureName" = 'LB'
        then (loss."runPotentialUnitsLost" * 0.45359237)
        when loss."runUnitOfMeasureName" = 'LBS'
        then (loss."runPotentialUnitsLost" * 0.45359237)
        when loss."runUnitOfMeasureName" = 'Kgs'
        then loss."runPotentialUnitsLost"
        when loss."runUnitOfMeasureName" is not null and conv.tovalue_norm is not null
        then
            (
                case
                    conv.touomname_norm
                    when 'Kgs'
                    then (loss."runPotentialUnitsLost" * tovalue_norm)
                    else ((loss."runPotentialUnitsLost" * tovalue_norm) * 0.45359237)
                end
            )
        else loss."potentialUnitsLost"
    end as kgs_potentialunitslost
from losses loss
left join
    conv conv
    on loss."siteUUID" = conv."siteUUID"
    and loss."productTypeUUID" = conv."productTypeUUID"
    and loss."runUnitOfMeasureUUID" = conv.fromuomuuid_norm
    and (
        conv.touomname_norm = 'LB'
        or conv.touomname_norm = 'LBS'
        or conv.touomname_norm = 'Kgs'
    )
