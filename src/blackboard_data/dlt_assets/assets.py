from dagster import AssetExecutionContext, AssetKey, AssetSpec, SourceAsset
from dagster_dlt import DagsterDltResource, dlt_assets, DagsterDltTranslator

from src.blackboard_data.resources.job_config import JobConfig
from src.blackboard_data.dlt_snowflake_to_fs.pipeline import (
    snowflake_data_source,
    snowflake_to_s3_pipeline,
)
from src.blackboard_data.dlt_fs_to_postgres.pipeline import (
    datalake_source,
    s3_to_postgres_pipeline,
)


snowflake_data_source_asset = SourceAsset(
    key="snowflake_data_source", group_name="EXTRACT"
)


class ExtractDagsterDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data) -> AssetSpec:
        default_spec = super().get_asset_spec(data)
        return default_spec.replace_attributes(deps=[snowflake_data_source_asset.key])


@dlt_assets(
    dlt_source=snowflake_data_source(),
    name="dlt_database_assets",
    group_name="EXTRACT",
    dlt_pipeline=snowflake_to_s3_pipeline,
    dagster_dlt_translator=ExtractDagsterDltTranslator(),
)
def extract(
    context: AssetExecutionContext, dlt_cli: DagsterDltResource, job_config: JobConfig
):
    extra_kwargs = {
        "write_disposition": job_config.dlt_write_disposition,
        "refresh": job_config.full_refresh,
        "loader_file_format": job_config.loader_file_format,
    }
    yield from dlt_cli.run(context=context, **extra_kwargs)


class LoadDagsterDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data) -> AssetSpec:
        default_spec = super().get_asset_spec(data)
        return default_spec.replace_attributes(
            deps=[AssetKey(f"dlt_snowflake_data_source_{data.resource.name}")]
        )


@dlt_assets(
    dlt_source=datalake_source(),
    name="dlt_filesystem_assets",
    group_name="LOAD",
    dlt_pipeline=s3_to_postgres_pipeline,
    dagster_dlt_translator=LoadDagsterDltTranslator(),
)
def load(
    context: AssetExecutionContext, dlt_cli: DagsterDltResource, job_config: JobConfig
):
    extra_kwargs = {
        "loader_file_format": job_config.loader_file_format,
    }
    yield from dlt_cli.run(context=context, **extra_kwargs)
