# flake8: noqa
import dlt
from dlt.sources.filesystem import filesystem, read_csv


def gen_files_resource(entity):
    new_files = filesystem(
        file_glob=f"blackboard_data/{entity}/*.csv",
        extract_content=True,
    )
    new_files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
    pipe = (new_files | read_csv(compression="gzip")).with_name(entity)
    return pipe


@dlt.source(parallelized=True)
def datalake_source():
    tables_to_load = ["person", "course", "person_course", "attempt"]
    for table_name in tables_to_load:
        yield gen_files_resource(table_name)


s3_to_postgres_pipeline = dlt.pipeline(
    pipeline_name="s3_to_postgres",
    dataset_name="blackboard_data",
    destination="postgres",
    progress="log",
)
