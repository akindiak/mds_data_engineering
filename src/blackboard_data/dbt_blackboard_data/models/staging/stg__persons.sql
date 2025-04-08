
with src as (
    select
        id,
        nullif({{ json_extract_path_text("stage", "user_id") }}, '') as user_name,
        nullif({{ json_extract_path_text("stage", "batch_uid") }}, '') as sis_id,
        nullif({{ json_extract_path_text("stage", "student_id") }}, '') as student_id,
        source_id,
        first_name,
        last_name,
        email,
        institution_role,
        institution_role_source_code,
        institution_role_source_desc,
        system_role,
        system_role_source_code,
        system_role_source_desc,
        department,
        available_ind,
        enabled_ind,
        {{ cast_to_utc("row_deleted_time") }} as deleted_at,
        {{ cast_to_utc("created_time") }} as created_at,
        {{ cast_to_utc("modified_time") }} as modified_at,
        row_number() over(partition by id order by coalesce(modified_time, created_time)) as rn
    from {{ source("blackboard_raw_data", "person") }}
)

select * from src
where rn = 1
