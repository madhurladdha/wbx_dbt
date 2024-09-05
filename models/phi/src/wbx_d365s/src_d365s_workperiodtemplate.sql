/* 
    Necessary changes applied on 8/29/24 by Mike Traub once the D365S table was available.

    This D365S src model has been modified as the underlying source replication table does not yet exist.
    The source table doesn't build if there is no source system data.
    For now, this is still pulling from the D365 source but filtering with the condition of 0=1 so that no data is passed.
    This needs to have the following udpates once the D365S table itself exists:
        -Change the source to source('D365S', 'workperiodtemplate')
        -Remove the 0=1 filter
        -Update the field mapping according to the D365S field names.
*/

with source as (

    select * from {{ source('D365S', 'workperiodtemplate') }}

),

renamed as (

    select
        legalentity as legalentity,
        name as name,
        fixeddaystart as fixeddaystart,
        worktimeid as worktimeid,
        upper(worktimeiddataareaid) as worktimeiddataareaid,
        legalentitydefault as legalentitydefault,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
