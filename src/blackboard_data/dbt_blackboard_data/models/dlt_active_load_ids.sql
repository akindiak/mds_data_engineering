/* check if the destination already has a list of processed ids
if not we will process all loads with status 0 (==success) */

{% set active_ids_exist = adapter.get_relation(
        database=this.database ,
        schema=this.schema,
        identifier="dlt_processed_load_ids" ) %}

select
    load_id
from blackboard_raw_data._dlt_loads
where status = 0
/* exclude already processed load_ids */
{% if active_ids_exist and not should_full_refresh() %}
and load_id not in (select load_id from {{ source('transformed_data', 'dlt_processed_load_ids') }})
{% endif %}