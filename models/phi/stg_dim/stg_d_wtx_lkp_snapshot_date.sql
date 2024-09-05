{{
    config(
        materialized=env_var("DBT_MAT_TABLE"),
        tags=["sales_snapshot_increment"]
    )
}}

/*  Want this as a table so that when it is refreshed, the snapshot date will hold until the next time it is run when we actually want to move the snapshot date forward 1. 
    This SHOULD NOT run or increment every time sales are run, hence no sales tag.
    It is referenced throughout the sales for anywhere that we are capturing snapshots.
*/

with source as (
    select * from {{ ref('src_sls_wtx_lkp_snapshot_date')}}
),
final as (
    select
        to_date(dateadd(hour,-12,current_timestamp)) as snapshot_date
    from source
)
select * from final