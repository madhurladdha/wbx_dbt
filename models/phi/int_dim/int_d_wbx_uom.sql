/* Mike Traub 5/29/2024
    For Weetabix UOM conversion there have been changes documented within the stage and dim models.
    The main change in this model is to give priority to the stg2 model as that is where the CA to KG conversion is calculated.
    The difference for D365 compared with AX is that the Net Weight is no longer used in D365 as it was in AX and so the code 
    must explicitly use the Net Weight for the CA->KG (and KG->CA) conversion.
*/

with stg_uom1 as(
    select * from {{ref('stg_d_wbx_uom')}}
),
stg_uom2 as(
    select * from {{ref('stg_d_wbx_uom2')}}
),

item_master as
(
    select * from {{ ref('dim_wbx_item') }}
),

stg1 as 

(
    select cast (item_id as varchar2 (255))          as item_id,
            cast (source_indicator as varchar2 (255)) as source_system,
            cast (from_uom as varchar2 (255))         as from_uom,
            cast (to_uom as varchar2 (255))           as to_uom,
            cast (conversion_rate as number (38, 10)) as conversion_rate,
            cast (inversion_rate as varchar2 (255))   as inversion_rate,
            effective_date                            as effective_date,
            expiration_date                           as expiration_date,
            cast (active_flag as varchar2 (255))      as active_flag
       from stg_uom1
       where (item_id,source_indicator,upper(from_uom),upper(to_uom)) not in 
       (select item_id,source_indicator,upper(from_uom),upper(to_uom) from stg_uom2 )
),

stg2 as (
    select cast (item_id as varchar2 (255))          as item_id,
            cast (source_indicator as varchar2 (255)) as source_system,
            cast (from_uom as varchar2 (255))         as from_uom,
            cast (to_uom as varchar2 (255))           as to_uom,
            cast (conversion_rate as number (38, 10)) as conversion_rate,
            cast (inversion_rate as varchar2 (255))   as inversion_rate,
            effective_date                            as effective_date,
            expiration_date                           as expiration_date,
            cast (active_flag as varchar2 (255))      as active_flag
       from  stg_uom2
),


int as
(
select * from stg1
union all
select * from stg2
),

final as(
    select item_id as source_item_identifier,
    int.source_system,
    from_uom,
    to_uom,
    conversion_rate,
    inversion_rate,
    effective_date,
    expiration_date,
    active_flag
    from int
    inner join item_master on int.item_id=item_master.source_item_identifier and int.source_system=item_master.source_system
    )

select  distinct * from final
