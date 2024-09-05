{{config(materialized='view',tags=["redzone","OEE", 'v_action'],)}}

with source as (

    select * from {{ source('weetabix-org', 'v_action') }}

),

renamed as (

    select
"siteId",
"timeZoneId",
"siteUUID",
"siteName",
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
"actionUUID",
"actionName",
"actionTitle",
"subject",
"targetType",
"highPriority",
"actionStatus",
"progress",
"displayId",
"externalId",
"actionDueDateTime",
"actionCompletedDateTime",
"actionDeadline",
"placeUUID",
"actionFromUUID",
"actionFromFirstName",
"actionFromLastName",
"actionOwnerUUID",
"actionOwnerFirstName",
"actionOwnerLastName",
"meetingTemplateUUID",
"meetingTemplateName",
"assetUUID",
"assetName",
"assetTypeName",
"sense",
"foodSafety",
"temporaryRepair",
"allDebrisRemoved",
"labels",
"triggeredByUUID",
"triggeredByName",
"triggerName",
"triggerConditionType",
"triggerConditionEventType",
"triggerConditionTimeType",
"triggerReactionType",
"actionCreatedDate",
"actionUpdatedDate"

    from source

)

select * from renamed

