{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        tags=["sales", "sales_fc_snapshot", "snapshot"],
        unique_key="unique_key",
        on_schema_change="sync_all_columns",
        incremental_strategy="merge",
        full_refresh=false,
    )
}}


/* Approach Used: Static Snapshot w/ Historical Conversion
    The approach used for this table is a Snapshot approach but also requires historical conversion from the old IICS data sets.
    Full details can be found in applicable documentation, but the highlights are provided here.
    1) References the old "conversion" or IICS data set for all snapshots up to the migration date.
    2) Environment variables used to drive the filtering so that the IICS data set is only pulled in on the initial run of the model in a new db/env.
    3) Same variables are used to drive filtering on the new (go-forward) data set
    4) End result should be that all old snapshots are captured and then this dbt model appends each new snapshot/version date to the data set in the dbt model.
    Other Design features:
    1) Model should NEVER be allowed to full-refresh.  This could wipe out all history.
    2) Model is incremental with unique_key = version date.  This ensures that past version dates are never deleted and re-runs on the same day will simply delete for
        the given version date and reload.
*/
with
    old_table as (
        select *
        from {{ ref("conv_sls_wbx_fc_snapshot_dim") }}
        {% if check_table_exists(this.schema, this.table) == "False" %}
            limit {{ env_var("DBT_NO_LIMIT") }}  -- --------Variable DBT_NO_LIMIT variable is set TO NULL to load everything from conv model if effective currency model is not present.
        {% else %} limit {{ env_var("DBT_LIMIT") }}  -- ---Variable DBT_LIMIT variable is set to 0 to load nothing if effective_currency table exist

        {% endif %}

    ),
    src as (select * from {{ ref("src_wbx_fc_snapshot") }}),
    dim as (
        select *
        from
            {% if check_table_exists(this.schema, this.table) == "False" %}
                {{ ref("conv_sls_wbx_fc_snapshot_dim") }}
            {% else %} {{ this }}

            {% endif %}
    ),
    src_final as (
        select
            to_date(snapshot_date, 'MM/DD/YYYY') as snapshot_date,
            snapshot_model,
            snapshot_code,
            snapshot_type,
            snapshot_desc,
            null as concensus_flag,
            null as purge_date
        from src
    ),

    final as (
        select
            '{{env_var("DBT_SOURCE_SYSTEM")}}' as source_system,
            src_final.snapshot_date as snapshot_date,
            src_final.snapshot_model as snapshot_model,
            src_final.snapshot_code as snapshot_code,
            src_final.snapshot_type as snapshot_type,
            src_final.snapshot_desc as snapshot_desc,
            src_final.concensus_flag as concensus_flag,
            current_timestamp as load_date,
            dim.update_date as dim_update_date,
            case
                when src_final.snapshot_type = 'WEEK_END'
                then dateadd(day, 400, src_final.snapshot_date)
                else
                    case
                        when src_final.purge_date is null
                        then null
                        else src_final.purge_date
                    end
            end as purge_date,
            case
                when
                    (
                        dim.snapshot_date is null
                        or dim.snapshot_model is null
                        or dim.snapshot_code is null
                        or dim.snapshot_type is null
                    )
                then 1
                else 0
            end dim_check_flag1,
            case
                when
                    (
                        (
                            nvl(rtrim(ltrim(src_final.snapshot_desc)), 'X')
                            <> nvl(rtrim(ltrim(dim.snapshot_desc)), 'X')
                        )
                        or (
                            nvl(rtrim(ltrim(src_final.concensus_flag)), 'X')
                            <> nvl(rtrim(ltrim(dim.concensus_flag)), 'X')
                        )
                        or (
                            nvl(rtrim(ltrim(src_final.purge_date)), 'X')
                            <> nvl(rtrim(ltrim(dim.purge_date)), 'X')
                        )
                    )
                then 1
                else 0
            end as dim_check_flag2
        from src_final
        left join
            dim
            on src_final.snapshot_date = dim.snapshot_date
            and src_final.snapshot_model = dim.snapshot_model
            and src_final.snapshot_code = dim.snapshot_code
            and src_final.snapshot_type = dim.snapshot_type

    ),

    old_model as (
        select
            cast(
                {{
                    dbt_utils.surrogate_key(
                        [
                            "SOURCE_SYSTEM",
                            "SNAPSHOT_DATE",
                            "SNAPSHOT_MODEL",
                            "SNAPSHOT_CODE",
                            "SNAPSHOT_TYPE",
                        ]
                    )
                }} as text(255)
            ) as unique_key,
            cast(substring(source_system, 1, 255) as text(255)) as source_system,
            cast(snapshot_date as date) as snapshot_date,
            cast(substring(snapshot_model, 1, 255) as text(255)) as snapshot_model,
            cast(substring(snapshot_code, 1, 255) as text(255)) as snapshot_code,
            cast(substring(snapshot_type, 1, 255) as text(255)) as snapshot_type,
            cast(substring(snapshot_desc, 1, 255) as text(255)) as snapshot_desc,
            cast(substring(concensus_flag, 1, 1) as text(1)) as concensus_flag,
            cast(to_date(purge_date) as date) as purge_date,
            cast(load_date as timestamp_ntz(9)) as load_date,
            cast(update_date as timestamp_ntz(9)) as update_date
        from old_table
    ),

    new_model as (
        select
            cast(
                {{
                    dbt_utils.surrogate_key(
                        [
                            "SOURCE_SYSTEM",
                            "SNAPSHOT_DATE",
                            "SNAPSHOT_MODEL",
                            "SNAPSHOT_CODE",
                            "SNAPSHOT_TYPE",
                        ]
                    )
                }} as text(255)
            ) as unique_key,
            cast(substring(source_system, 1, 255) as text(255)) as source_system,
            cast(snapshot_date as date) as snapshot_date,
            cast(substring(snapshot_model, 1, 255) as text(255)) as snapshot_model,
            cast(substring(snapshot_code, 1, 255) as text(255)) as snapshot_code,
            cast(substring(snapshot_type, 1, 255) as text(255)) as snapshot_type,
            cast(substring(snapshot_desc, 1, 255) as text(255)) as snapshot_desc,
            cast(substring(concensus_flag, 1, 1) as text(1)) as concensus_flag,
            cast(purge_date as timestamp_ntz(9)) as purge_date,
            cast(load_date as timestamp_ntz(9)) as load_date,
            cast(
                case
                    when dim_check_flag1 = 1
                    then load_date
                    when dim_check_flag2 = 1
                    then load_date
                    else dim_update_date
                end as timestamp_ntz(9)
            ) as update_date
        from final

    ),

    base_dim as (
        select *
        from new_model
        {% if check_table_exists(this.schema, this.table) == "True" %}
            limit {{ env_var("DBT_NO_LIMIT") }}
        {% else %} limit {{ env_var("DBT_LIMIT") }}
        {% endif %}
    )

select *
from base_dim
union
select *
from old_model
