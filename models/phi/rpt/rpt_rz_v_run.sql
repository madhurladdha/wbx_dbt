{{
    config(
        materialized=env_var("DBT_RZ_DS_MAT"),
        snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
        tags=[
            "redzone",
            "OEE",
            "v_productunitconversion",
            "v_run",
        ],
    )
}}

with
    conv as (select * from {{ ref("v_rz_productunitconversion_norm") }}),

    v_run as (select * from {{ ref("src_rz_v_run") }}),

    cp as (
        select
            to_boolean(ifnull(normalized_value, false)) as choke_point,
            source_value,
            source_site
        from {{ ref("src_rz_wbx_normalization") }}
        where reference_field = 'OEE_INCLUDE'
    ),

    run as (
        select v_run.*, ifnull(cp.choke_point, false) as choke_point
        from v_run
        left join
            cp
            on v_run."siteName" = cp.source_site
            and v_run."locationName" = cp.source_value
    )

select
    run."siteId",
    run."timeZoneId",
    run."dateYear",
    run."quarter",
    run."quarterName",
    run."monthNumber",
    run."monthName",
    run."week",
    run."dayName",
    run."dayOfWeekNumber",
    run."dayOfWeekNumberIso",
    run."hourOfDay",
    run."dateTimeNearestHour",
    run."areaUUID",
    run."areaName",
    run."siteUUID",
    run."siteName",
    run."siteUnitOfMeasureUUID",
    run."siteUnitOfMeasureName",
    run."runUUID",
    run."runName",
    run."runScheduledStartTime",
    run."runStartTime",
    run."runEndTime",
    run."unitOfMeasureUUID",
    run."unitOfMeasureName",
    run."manufacturingTypeUUID",
    run."manufacturingTypeName",
    run."autoStopped",
    run."averageCrewSize",
    run."averageStandardRatePerMin",
    run."averageStandardRatePerManHour",
    run."plannedQuantity",
    run."manHours",
    run."productTypeUUID",
    run."productTypeName",
    run."productTypeSKU",
    run."productTypeLabelWeight",
    run."locationUUID",
    run."locationName",
    run."sensorConfigurationUUID",
    run."sensorConfigurationMode",
    run."sensorConfigurationName",
    run."customReference",
    run."productionType",
    run."runId",
    run."locked",
    run."hasQualityChecks",
    run."signatureOnHold",
    run."splitSource",
    run."splitRemainder",
    run."runSignatureUsername",
    run."runSignatureLastName",
    run."runSignatureFirstName",
    run."runSignatureTime",
    run."runSignatureType",
    run."inCount",
    run."outCount",
    run."siteOutputQuantity",
    run."theoreticalQuantity",
    run."oee",
    run."target",
    run."offsetTarget",
    run."edgeUpTarget",
    run."minimumTarget",
    run."targetQuantity",
    run."offsetTargetQuantity",
    run."edgeUpTargetQuantity",
    run."minimumTargetQuantity",
    run."quality",
    run."availability",
    run."performance",
    run."upSeconds",
    run."downSeconds",
    run."plannedDownSeconds",
    run."leadOperatorUUID",
    run."leadOperatorUsername",
    run."leadOperatorFirstName",
    run."leadOperatorLastName",
    run."leadOperatorStartTime",
    run."reportingInCount",
    run."reportingOutCount",
    run."reportingTheoreticalQuantity",
    run."reportingTargetQuantity",
    run."reportingOffsetTargetQuantity",
    run."reportingEdgeUpTargetQuantity",
    run."reportingMinimumTargetQuantity",
    run."rolledThroughputYield",
    run.choke_point,
    case
        when run."unitOfMeasureName" = 'LB'
        then run."theoreticalQuantity"
        when run."unitOfMeasureName" = 'LBS'
        then run."theoreticalQuantity"
        when run."unitOfMeasureName" = 'Kgs'
        then (run."theoreticalQuantity" * 2.20462)
        when run."unitOfMeasureName" is not null and conv.tovalue_norm is not null
        then
            (
                case
                    conv.touomname_norm
                    when 'Kgs'
                    then ((run."theoreticalQuantity" * tovalue_norm) * 2.20462)
                    else (run."theoreticalQuantity" * tovalue_norm)
                end
            )
        else run."theoreticalQuantity"
    end as lbs_theoreticalquantity,
    case
        when run."unitOfMeasureName" = 'LB'
        then (run."theoreticalQuantity" * 0.45359237)
        when run."unitOfMeasureName" = 'LBS'
        then (run."theoreticalQuantity" * 0.45359237)
        when run."unitOfMeasureName" = 'Kgs'
        then run."theoreticalQuantity"
        when run."unitOfMeasureName" is not null and conv.tovalue_norm is not null
        then
            (
                case
                    conv.touomname_norm
                    when 'Kgs'
                    then (run."theoreticalQuantity" * tovalue_norm)
                    else ((run."theoreticalQuantity" * tovalue_norm) * 0.45359237)
                end
            )
        else run."theoreticalQuantity"
    end as kgs_theoreticalquantity,
    case
        when run."unitOfMeasureName" = 'LB'
        then run."outCount"
        when run."unitOfMeasureName" = 'LBS'
        then run."outCount"
        when run."unitOfMeasureName" = 'Kgs'
        then (run."outCount" * 2.20462)
        when run."unitOfMeasureName" is not null and conv.tovalue_norm is not null
        then
            (
                case
                    conv.touomname_norm
                    when 'Kgs'
                    then ((run."outCount" * tovalue_norm) * 2.20462)
                    else (run."outCount" * tovalue_norm)
                end
            )
        else run."outCount"
    end as lbs_outcount,
    case
        when run."unitOfMeasureName" = 'LB'
        then (run."outCount" * 0.45359237)
        when run."unitOfMeasureName" = 'LBS'
        then (run."outCount" * 0.45359237)
        when run."unitOfMeasureName" = 'Kgs'
        then run."outCount"
        when run."unitOfMeasureName" is not null and conv.tovalue_norm is not null
        then
            (
                case
                    conv.touomname_norm
                    when 'Kgs'
                    then (run."outCount" * tovalue_norm)
                    else ((run."outCount" * tovalue_norm) * 0.45359237)
                end
            )
        else run."outCount"
    end as kgs_outcount,
    case
        when run."unitOfMeasureName" = 'LB'
        then run."inCount"
        when run."unitOfMeasureName" = 'LBS'
        then run."inCount"
        when run."unitOfMeasureName" = 'Kgs'
        then (run."inCount" * 2.20462)
        when run."unitOfMeasureName" is not null and conv.tovalue_norm is not null
        then
            (
                case
                    conv.touomname_norm
                    when 'Kgs'
                    then ((run."inCount" * tovalue_norm) * 2.20462)
                    else (run."inCount" * tovalue_norm)
                end
            )
        else run."inCount"
    end as lbs_incount,
    case
        when run."unitOfMeasureName" = 'LB'
        then (run."inCount" * 0.45359237)
        when run."unitOfMeasureName" = 'LBS'
        then (run."inCount" * 0.45359237)
        when run."unitOfMeasureName" = 'Kgs'
        then run."inCount"
        when run."unitOfMeasureName" is not null and conv.tovalue_norm is not null
        then
            (
                case
                    conv.touomname_norm
                    when 'Kgs'
                    then (run."inCount" * tovalue_norm)
                    else ((run."inCount" * tovalue_norm) * 0.45359237)
                end
            )
        else run."inCount"
    end as kgs_incount,
    case
        when run."unitOfMeasureName" = 'LB'
        then run."targetQuantity"
        when run."unitOfMeasureName" = 'LBS'
        then run."targetQuantity"
        when run."unitOfMeasureName" = 'Kgs'
        then (run."targetQuantity" * 2.20462)
        when run."unitOfMeasureName" is not null and conv.tovalue_norm is not null
        then
            (
                case
                    conv.touomname_norm
                    when 'Kgs'
                    then ((run."targetQuantity" * tovalue_norm) * 2.20462)
                    else (run."targetQuantity" * tovalue_norm)
                end
            )
        else run."targetQuantity"
    end as lbs_target_quantity,
    case
        when run."unitOfMeasureName" = 'LB'
        then (run."targetQuantity" * 0.45359237)
        when run."unitOfMeasureName" = 'LBS'
        then (run."targetQuantity" * 0.45359237)
        when run."unitOfMeasureName" = 'Kgs'
        then run."targetQuantity"
        when run."unitOfMeasureName" is not null and conv.tovalue_norm is not null
        then
            (
                case
                    conv.touomname_norm
                    when 'Kgs'
                    then (run."targetQuantity" * tovalue_norm)
                    else ((run."targetQuantity" * tovalue_norm) * 0.45359237)
                end
            )
        else run."targetQuantity"
    end as kgs_target_quantity,
    conv.tovalue_norm,
    conv.touomname_norm,
    conv.fromuomname_norm
from run
left join
    conv conv
    on run."siteUUID" = conv."siteUUID"
    and run."productTypeUUID" = conv."productTypeUUID"
    and run."unitOfMeasureUUID" = conv.fromuomuuid_norm
    and (
        conv.touomname_norm = 'LB'
        or conv.touomname_norm = 'LBS'
        or conv.touomname_norm = 'Kgs'
    )
