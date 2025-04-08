from dagster import file_relative_path
from dagster_dbt import DbtProject

DBT_PROJECT_PATH = file_relative_path(__file__, "dbt_blackboard_data")
DBT_PROFILE = "dbt_blackboard_data"
DBT_TARGET = "prod"

dbt_project = DbtProject(project_dir=DBT_PROJECT_PATH, target=DBT_TARGET)
