# flake8: noqa
import dlt
import pendulum
from dlt.sources.sql_database import sql_table
import sqlalchemy as sa
from sqlalchemy.sql import sqltypes
from snowflake.sqlalchemy import TIMESTAMP_LTZ
from dlt import pipeline
from src.blackboard_data.configs import BLACKBOARD_DATA_TABLES_TO_LOAD


def type_adapter_callback(sql_type):
    if isinstance(
        sql_type, TIMESTAMP_LTZ
    ):  # Snowflake does not inherit from sa.DateTime
        return sa.DateTime(timezone=True)
    return sql_type  # Use default detection for other types


def add_max_timestamp(table):
    computed_max_timestamp = sa.sql.type_coerce(
        sa.func.greatest(
            sa.func.coalesce(
                table.c.row_inserted_time, pendulum.DateTime(1970, 1, 1, 0, 0, 0)
            ),
            sa.func.coalesce(
                table.c.row_updated_time, pendulum.DateTime(1970, 1, 1, 0, 0, 0)
            ),
            sa.func.coalesce(
                table.c.row_deleted_time, pendulum.DateTime(1970, 1, 1, 0, 0, 0)
            ),
        ),
        sqltypes.TIMESTAMP,
    ).label("_max_timestamp")
    subquery = sa.select(*table.c, computed_max_timestamp).subquery()
    return subquery


def gen_table_resource(table_name):
    resource = sql_table(
        schema="CDM_LMS",
        table=table_name,
        reflection_level="full",
        backend="pyarrow",
        backend_kwargs={"tz": "UTC"},
        defer_table_reflect=True,
        table_adapter_callback=add_max_timestamp,
        type_adapter_callback=type_adapter_callback,
        incremental=dlt.sources.incremental(
            "_max_timestamp",
            initial_value=pendulum.DateTime(1970, 1, 1, 0, 0, 0, tzinfo=pendulum.UTC),
        ),
    )
    return resource


@dlt.source(parallelized=True)
def snowflake_data_source():
    for table_name in BLACKBOARD_DATA_TABLES_TO_LOAD:
        yield gen_table_resource(table_name)


snowflake_to_s3_pipeline = pipeline(
    pipeline_name="snowflake_to_s3",
    dataset_name="blackboard_data",
    destination="filesystem",
    progress="log",
)
