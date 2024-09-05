{{
    config(
        materialized=env_var("DBT_RZ_DS_MAT"),
        snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
        tags=["redzone", "OEE", "v_productunitconversion", "v_shiftrunsegment"],
    )
}}

with
    conv as (select * from {{ ref("v_rz_productunitconversion_norm") }}),

    shiftrunseg as (select * from {{ ref("src_rz_v_shiftrunsegment") }}),

    cp as (
        select
            to_boolean(ifnull(normalized_value, false)) as choke_point,
            source_value,
            source_site
        from {{ ref("src_rz_wbx_normalization") }}
        where reference_field = 'OEE_INCLUDE'
    ),

    shiftrunsegment as (
        select shiftrunseg.*, ifnull(choke_point, false) as choke_point
        from shiftrunseg
        left join
            cp
            on shiftrunseg."locationName" = cp.source_value
            and shiftrunseg."siteName" = cp.source_site
    )

select
    seg."siteId",
    seg."timeZoneId",
    seg."dateYear",
    seg."quarter",
    seg."quarterName",
    seg."monthNumber",
    seg."monthName",
    seg."week",
    seg."dayName",
    seg."dayOfWeekNumber",
    seg."dayOfWeekNumberIso",
    seg."hourOfDay",
    seg."dateTimeNearestHour",
    seg."areaUUID",
    seg."areaName",
    seg."siteUUID",
    seg."siteName",
    seg."siteUnitOfMeasureUUID",
    seg."siteUnitOfMeasureName",
    seg."shiftUUID",
    seg."shiftName",
    seg."startTime",
    seg."endTime",
    seg."runUnitOfMeasureUUID",
    seg."runUnitOfMeasureName",
    seg."shiftUnitOfMeasureUUID",
    seg."shiftUnitOfMeasureName",
    seg."locationUUID",
    seg."locationName",
    seg."productionType",
    seg."runUUID",
    seg."runName",
    seg."productTypeUUID",
    seg."customReference",
    seg."runId",
    seg."locked",
    seg."hasQualityChecks",
    seg."signatureOnHold",
    seg."runStandardRatePerMin",
    seg."productTypeName",
    seg."productTypeSKU",
    seg."productTypeLabelWeight",
    seg."manufacturingTypeUUID",
    seg."manufacturingTypeName",
    seg."runSignatureUsername",
    seg."runSignatureLastName",
    seg."runSignatureFirstName",
    seg."runSignatureTime",
    seg."runSignatureType",
    seg."inCount",
    seg."outCount",
    seg."siteOutputQuantity",
    seg."theoreticalQuantity",
    seg."oee",
    seg."target",
    seg."offsetTarget",
    seg."edgeUpTarget",
    seg."minimumTarget",
    seg."targetQuantity",
    seg."offsetTargetQuantity",
    seg."edgeUpTargetQuantity",
    seg."minimumTargetQuantity",
    seg."quality",
    seg."availability",
    seg."performance",
    seg."upSeconds",
    seg."downSeconds",
    seg."plannedDownSeconds",
    seg."manHours",
    seg."runInCount",
    seg."runOutCount",
    seg."runTheoreticalQuantity",
    seg."runTargetQuantity",
    seg."runOffsetTargetQuantity",
    seg."runEdgeUpTargetQuantity",
    seg."runMinimumTargetQuantity",
    seg."createdDate",
    seg."updatedDate",
    seg."leadOperatorUUID",
    seg."leadOperatorUsername",
    seg."leadOperatorFirstName",
    seg."leadOperatorLastName",
    seg."reportingInCount",
    seg."reportingOutCount",
    seg."reportingTheoreticalQuantity",
    seg."reportingTargetQuantity",
    seg."reportingOffsetTargetQuantity",
    seg."reportingEdgeUpTargetQuantity",
    seg."reportingMinimumTargetQuantity",
    seg."CHOKE_POINT",
    case
        when seg."shiftUnitOfMeasureName" = 'LB'
        then seg."theoreticalQuantity"
        when seg."shiftUnitOfMeasureName" = 'LBS'
        then seg."theoreticalQuantity"
        when seg."shiftUnitOfMeasureName" = 'Kgs'
        then (seg."theoreticalQuantity" * 2.20462)
        when seg."runUnitOfMeasureName" = 'LB'
        then seg."runTheoreticalQuantity"
        when seg."runUnitOfMeasureName" = 'LBS'
        then seg."runTheoreticalQuantity"
        when seg."runUnitOfMeasureName" = 'Kgs'
        then (seg."runTheoreticalQuantity" * 2.20462)
        when seg."runUnitOfMeasureName" is not null and conv.tovalue_norm is not null
        then
            (
                case
                    conv.touomname_norm
                    when 'Kgs'
                    then ((seg."runTheoreticalQuantity" * tovalue_norm) * 2.20462)
                    else (seg."runTheoreticalQuantity" * tovalue_norm)
                end
            )
        else seg."theoreticalQuantity"
    end as lbs_theoreticalquantity,
    case
        when seg."shiftUnitOfMeasureName" = 'LB'
        then (seg."theoreticalQuantity" * 0.45359237)
        when seg."shiftUnitOfMeasureName" = 'LBS'
        then (seg."theoreticalQuantity" * 0.45359237)
        when seg."shiftUnitOfMeasureName" = 'Kgs'
        then seg."theoreticalQuantity"
        when seg."runUnitOfMeasureName" = 'LB'
        then (seg."runTheoreticalQuantity" * 0.45359237)
        when seg."runUnitOfMeasureName" = 'LBS'
        then (seg."runTheoreticalQuantity" * 0.45359237)
        when seg."runUnitOfMeasureName" = 'Kgs'
        then seg."runTheoreticalQuantity"
        when seg."runUnitOfMeasureName" is not null and conv.tovalue_norm is not null
        then
            (
                case
                    conv.touomname_norm
                    when 'Kgs'
                    then (seg."runTheoreticalQuantity" * tovalue_norm)
                    else ((seg."runTheoreticalQuantity" * tovalue_norm) * 0.45359237)
                end
            )
        else seg."theoreticalQuantity"
    end as kgs_theoreticalquantity,
    case
        when seg."shiftUnitOfMeasureName" = 'LB'
        then seg."inCount"
        when seg."shiftUnitOfMeasureName" = 'LBS'
        then seg."inCount"
        when seg."shiftUnitOfMeasureName" = 'Kgs'
        then (seg."inCount" * 2.20462)
        when seg."runUnitOfMeasureName" = 'LB'
        then seg."runInCount"
        when seg."runUnitOfMeasureName" = 'LBS'
        then seg."runInCount"
        when seg."runUnitOfMeasureName" = 'Kgs'
        then (seg."runInCount" * 2.20462)
        when seg."runUnitOfMeasureName" is not null and conv.tovalue_norm is not null
        then
            (
                case
                    conv.touomname_norm
                    when 'Kgs'
                    then ((seg."runInCount" * tovalue_norm) * 2.20462)
                    else (seg."runInCount" * tovalue_norm)
                end
            )
        else seg."inCount"
    end as lbs_incount,
    case
        when seg."shiftUnitOfMeasureName" = 'LB'
        then (seg."inCount" * 0.45359237)
        when seg."shiftUnitOfMeasureName" = 'LBS'
        then (seg."inCount" * 0.45359237)
        when seg."shiftUnitOfMeasureName" = 'Kgs'
        then seg."inCount"
        when seg."runUnitOfMeasureName" = 'LB'
        then (seg."runInCount" * 0.45359237)
        when seg."runUnitOfMeasureName" = 'LBS'
        then (seg."runInCount" * 0.45359237)
        when seg."runUnitOfMeasureName" = 'Kgs'
        then seg."runInCount"
        when seg."runUnitOfMeasureName" is not null and conv.tovalue_norm is not null
        then
            (
                case
                    conv.touomname_norm
                    when 'Kgs'
                    then (seg."runInCount" * tovalue_norm)
                    else ((seg."runInCount" * tovalue_norm) * 0.45359237)
                end
            )
        else seg."inCount"
    end as kgs_incount,
    case
        when seg."shiftUnitOfMeasureName" = 'LB'
        then seg."outCount"
        when seg."shiftUnitOfMeasureName" = 'LBS'
        then seg."outCount"
        when seg."shiftUnitOfMeasureName" = 'Kgs'
        then (seg."outCount" * 2.20462)
        when seg."runUnitOfMeasureName" = 'LB'
        then seg."runOutCount"
        when seg."runUnitOfMeasureName" = 'LBS'
        then seg."runOutCount"
        when seg."runUnitOfMeasureName" = 'Kgs'
        then (seg."runOutCount" * 2.20462)
        when seg."runUnitOfMeasureName" is not null and conv.tovalue_norm is not null
        then
            (
                case
                    conv.touomname_norm
                    when 'Kgs'
                    then ((seg."runOutCount" * tovalue_norm) * 2.20462)
                    else (seg."runOutCount" * tovalue_norm)
                end
            )
        else seg."outCount"
    end as lbs_outcount,
    case
        when seg."shiftUnitOfMeasureName" = 'LB'
        then (seg."outCount" * 0.45359237)
        when seg."shiftUnitOfMeasureName" = 'LBS'
        then (seg."outCount" * 0.45359237)
        when seg."shiftUnitOfMeasureName" = 'Kgs'
        then seg."outCount"
        when seg."runUnitOfMeasureName" = 'LB'
        then (seg."runOutCount" * 0.45359237)
        when seg."runUnitOfMeasureName" = 'LBS'
        then (seg."runOutCount" * 0.45359237)
        when seg."runUnitOfMeasureName" = 'Kgs'
        then seg."runOutCount"
        when seg."runUnitOfMeasureName" is not null and conv.tovalue_norm is not null
        then
            (
                case
                    conv.touomname_norm
                    when 'Kgs'
                    then (seg."runOutCount" * tovalue_norm)
                    else ((seg."runOutCount" * tovalue_norm) * 0.45359237)
                end
            )
        else seg."outCount"
    end as kgs_outcount,

    conv.touomname_norm,
    conv.fromuomname_norm,
    conv.tovalue_norm
from shiftrunsegment seg
left join
    conv conv
    on seg."siteUUID" = conv."siteUUID"
    and seg."productTypeUUID" = conv."productTypeUUID"
    and seg."runUnitOfMeasureUUID" = conv.fromuomuuid_norm
    and (
        conv.touomname_norm = 'LB'
        or conv.touomname_norm = 'LBS'
        or conv.touomname_norm = 'Kgs'
    )
