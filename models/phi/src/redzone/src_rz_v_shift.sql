{{ config(      materialized='view',tags=["redzone","OEE", 'v_shift'],    ) }}

with
    source as (select * from {{ source("weetabix-org", "v_shift") }}),

    renamed as (

        select

            "siteId",
            "timeZoneId",
            "dateYear",
            "quarter",
            "quarterName",
            "monthNumber",
            "monthName",
            "week",
            "dayName",
            "dayOfWeekNumber",
            "dayOfWeekNumberIso",
            "hourOfDay",
            "dateTimeNearestHour",
            "areaUUID",
            "areaName",
            "siteUUID",
            "siteName",
            "siteUnitOfMeasureUUID",
            "siteUnitOfMeasureName",
            "shiftUUID",
            "shiftName",
            "startTime",
            "endTime",
            "unitOfMeasureUUID",
            "unitOfMeasureName",
            "locationUUID",
            "locationName",
            "shiftTypeUUID",
            "name",
            "planStartTime",
            "planEndTime",
            "runsOnDays",
            "autoStopped",
            "productionType",
            "startOffsetMinutes",
            "rampUpMinutes",
            "rampUpTargetMinutes",
            "inCount",
            "outCount",
            "siteOutputQuantity",
            "theoreticalQuantity",
            "oee",
            "target",
            "offsetTarget",
            "edgeUpTarget",
            "minimumTarget",
            "targetQuantity",
            "offsetTargetQuantity",
            "edgeUpTargetQuantity",
            "minimumTargetQuantity",
            "quality",
            "availability",
            "performance",
            "upSeconds",
            "downSeconds",
            "plannedDownSeconds",
            "manHours",
            "leadOperatorUUID",
            "leadOperatorUsername",
            "leadOperatorFirstName",
            "leadOperatorLastName",
            "reportingInCount",
            "reportingOutCount",
            "reportingTheoreticalQuantity",
            "reportingTargetQuantity",
            "reportingOffsetTargetQuantity",
            "reportingEdgeUpTargetQuantity",
            "reportingMinimumTargetQuantity",
            "productCount",
            "rolledThroughputYield"

        from source
    )

select *
from renamed
