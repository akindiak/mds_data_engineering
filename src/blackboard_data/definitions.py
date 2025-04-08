import dagster as dg
from src.blackboard_data import dlt_assets
from src.blackboard_data import dbt_assets
from dagster_dbt import DbtCliResource
from dagster_dlt import DagsterDltResource
from src.blackboard_data.resources.job_config import JobConfig
from src.blackboard_data.jobs import main_job
from src.blackboard_data.dbt_project_cfg import dbt_project, DBT_PROFILE, DBT_TARGET


dbt_project.prepare_if_dev()

_datahub_assets = dg.load_assets_from_package_module(
    package_module=dlt_assets,
)
_dbt_assets = dg.load_assets_from_package_module(
    package_module=dbt_assets, group_name="TRANSFORM"
)

defs = dg.Definitions(
    assets=[*_datahub_assets, *_dbt_assets],
    jobs=[main_job],
    resources={
        "dbt": DbtCliResource(
            project_dir=dbt_project,
            profile=DBT_PROFILE,
            target=DBT_TARGET,
        ),
        "dlt_cli": DagsterDltResource(),
        "job_config": JobConfig(),
    },
)
