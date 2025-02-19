import dagster as dg
from typing import Literal


class JobConfig(dg.ConfigurableResource):
    full_refresh: bool = False
    dlt_write_disposition: Literal["append", "replace", "merge"] = "append"
    loader_file_format: Literal["parquet", "jsonl", "csv"] = "csv"
