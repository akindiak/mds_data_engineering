from typing import Mapping, Any

import dagster as dg
from dagster import AssetKey
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator
from src.blackboard_data.dbt_project_cfg import dbt_project
from src.blackboard_data.resources.job_config import JobConfig


class TransformDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey | None:
        if dbt_resource_props["resource_type"] == "source":
            return AssetKey(f"dlt_datalake_source_{dbt_resource_props['name']}")
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=TransformDagsterDbtTranslator(),
)
def transform(
    context: dg.OpExecutionContext,
    dbt: DbtCliResource,
    job_config: JobConfig,
):
    dbt_args = ["build"]

    if job_config.full_refresh:
        dbt_args += ["--full-refresh"]

    yield from dbt.cli(dbt_args, context=context)
