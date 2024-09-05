{{
    config(
        materialized="view",
        tags=["redzone", "OEE", "v_completeddatasheet"]
    )
}}

with source as (

    select * from {{ source('weetabix-org', 'v_completeddatasheet') }}

),

renamed as (

select
"shiftName",
"approvalUserSignatureFirstName",
"completeTime",
"placeType",
"hourOfDay",
"productSku",
"observationUserSignatureLastName",
"userSignatureFirstName",
"dataSheetType",
"placeName",
"productOverheadCost",
"shiftUUID",
"productCost",
"deactivatedUserSignatureLastName",
"lotCode",
"runName",
"quarterName",
"stationUUID",
"void",
"dayOfWeekNumber",
"siteId",
"auditScore",
"userSignatureUserName",
"dataSheetId",
"reauthenticated",
"includeInRunSummary",
"productTypeUUID",
"description",
"siteName",
"replacesDataSheetUUID",
"stationName",
"dayName",
"observationTime",
"monthNumber",
"dataSheetTemplateUUID",
"ccp",
"stepUUID",
"deactivatedTime",
"createdDate",
"dayOfWeekNumberIso",
"actionUUID",
"deactivatedUserSignatureUserName",
"observationUserSignatureUUID",
"productName",
"approvalUserSignatureUUID",
"cycleUUID",
"userSignatureUUID",
"dataSheetName",
"observationUserSignatureFirstName",
"assetName",
"resultType",
"productExternalId",
"siteUUID",
"dateTimeNearestHour",
"deactivatedUserSignatureFirstName",
"approvalTime",
"runId",
"placeUUID",
"approvalUserSignatureLastName",
"updatedDate",
"dateYear",
"scoringRule",
"stepName",
"productStandardRatePerMinute",
"approvalUserSignatureUserName",
"draftTime",
"productMaterialCost",
"week",
"userSignatureLastName",
"runUUID",
"productUnitOfMeasureName",
"customReference",
"timeZoneId",
"observationUserSignatureUserName",
"assetTypeName",
"productUUID",
"deactivatedUserSignatureUUID",
"passScore",
"monthName",
"quarter"

from source

)

select * from renamed
