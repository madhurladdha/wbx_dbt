{{
    config(
        materialized="view",
        tags=["redzone", "OEE", "v_comment"],
    )
}}

with source as (

    select * from {{ source('weetabix-org', 'v_comment') }}

),

renamed as (

select
"commentUUID",
"siteName",
"ownerUUID",
"commentHighlightType",
"quarter",
"siteId",
"username",
"timeZoneId",
"createdDate",
"firstName",
"dayName",
"lastName",
"siteUUID",
"triggeredByUUID",
"dateTimeNearestHour",
"targetUUID",
"dateYear",
"threadChatUUID",
"updatedDate",
"linkType",
"targetType",
"dayOfWeekNumber",
"dayOfWeekNumberIso",
"systemCommentType",
"week",
"description",
"monthNumber",
"quarterName",
"hourOfDay",
"monthName"
from source

)

select * from renamed