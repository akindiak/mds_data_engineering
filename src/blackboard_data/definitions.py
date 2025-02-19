import dagster as dg
from src.blackboard_data import dlt_assets
from dagster_dlt import DagsterDltResource
from src.blackboard_data.resources.job_config import JobConfig
from src.blackboard_data.jobs import main_job


_datahub_assets = dg.load_assets_from_package_module(
    package_module=dlt_assets,
)

defs = dg.Definitions(
    assets=[*_datahub_assets],
    jobs=[main_job],
    resources={
        "dlt_cli": DagsterDltResource(),
        "job_config": JobConfig(),
    },
)
