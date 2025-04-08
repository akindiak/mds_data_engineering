{% macro json_extract_path_text(json, key) %}
   {{ return(adapter.dispatch('json_extract_path_text', 'dbt_blackboard_data')(json, key)) }}
{% endmacro %}

{% macro default__json_extract_path_text(json, key) %}
{% if key is sequence and key is not string %}
JSON_EXTRACT_PATH_TEXT(
    {{ json }}::json, {% for k in key %}'{{ k }}'{% if not loop.last %}, {% endif %}{% endfor %}
)
{% else %}
JSON_EXTRACT_PATH_TEXT({{ json }}::json, '{{ key }}')
{% endif %}
{% endmacro %}

{% macro postgres__json_extract_path_text(json, key) %}
{% if key is sequence and key is not string %}
JSON_EXTRACT_PATH_TEXT(
    {{ json }}::json, {% for k in key %}'{{ k }}'{% if not loop.last %}, {% endif %}{% endfor %}
)
{% else %}
JSON_EXTRACT_PATH_TEXT({{ json }}::json, '{{ key }}')
{% endif %}
{% endmacro %}

{% macro redshift__json_extract_path_text(json, key) %}
{% if key is sequence and key is not string %}
JSON_EXTRACT_PATH_TEXT(
    {{ json }}::varchar(max), {% for k in key %}'{{ k }}'{% if not loop.last %}, {% endif %}{% endfor %},
    true
)
{% else %}
JSON_EXTRACT_PATH_TEXT({{ json }}::varchar(max), '{{ key }}', true)
{% endif %}
{% endmacro %}