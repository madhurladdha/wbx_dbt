with source as (

    select * from {{ source('WEETABIX', 'EXC_Dim_Users') }}

),

renamed as (

    select
USER_IDX,
USER_FIRSTNAME,
USER_LASTNAME,
USER_DISPLAYNAME,
USER_EMAIL,
USER_LOGINNAME,
USER_ENCRYPTEDPASSWORD,
USER_UNIQUECODE,
USER_SSONAME,
USER_EMAILSENABLED,
USER_EMAILFREQUENCY,
USER_EMAILDIGEST,
USER_EMAILLASTSENTTS,
USER_AD_NAME,
USER_PASSWORDEXPIRESON,
ISENABLED,
ISUSERPROFILE,
ASSIGNEDPROFILE_IDX,
USER_LANGUAGE,
SESSIONID,
USER_ACCENT,
DATE_CREATED,
ISEXCEEDRAUSER

    from source

)

select * from renamed