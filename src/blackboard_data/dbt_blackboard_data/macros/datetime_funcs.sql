{% macro cast_to_utc(col) %}
   {{ return(adapter.dispatch('cast_to_utc', 'dbt_blackboard_data')(col)) }}
{% endmacro %}

{% macro default__cast_to_utc(col) %}
    ("{{ col }}" at time zone 'UTC')
{% endmacro %}