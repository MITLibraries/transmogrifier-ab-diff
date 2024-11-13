import logging
from collections.abc import Generator
from pathlib import Path

import duckdb
import pyarrow as pa

from abdiff.config import Config
from abdiff.core.utils import (
    load_dataset,
    read_run_json,
    update_or_create_run_json,
    write_to_dataset,
)

logger = logging.getLogger(__name__)

CONFIG = Config()

READ_BATCH_SIZE = 1_000


def create_final_records(
    run_directory: str, diffs_dataset_path: str, metrics_dataset_path: str
) -> str:
    """Produce a single, final dataset that contains all records and diff information.

    This dataset is produced by joining the "diffs" dataset (which contains the full
    A and B records, and the JSON diff) with the "metrics" dataset (which is a sparse
    matrix of TIMDEX fields and boolean 1 or 0 if that record has a diff for that field).
    This dataset should be sufficient for supporting any webapp data needs.

    This dataset is partitioned by source and 'has_diff' boolean.

    Lastly, a DuckDB database file is created with some views and small convenience tables
    for the webapp to use.
    """
    logger.info("Creating final records dataset from 'diffs' and 'metrics' datasets.")
    run_data = read_run_json(run_directory)
    metrics_timdex_field_columns = run_data["metrics"]["summary"]["fields_with_diffs"]

    # get list of unique columns from metrics dataset, and create final dataset schema
    metrics_dataset = load_dataset(metrics_dataset_path)
    metrics_columns = (
        pa.field(name, pa.int64())
        for name in metrics_dataset.schema.names
        if name in metrics_timdex_field_columns
    )
    final_records_dataset_schema = pa.schema(
        (
            pa.field("timdex_record_id", pa.string()),
            pa.field("source", pa.string()),
            pa.field("record_a", pa.binary()),
            pa.field("record_b", pa.binary()),
            pa.field("ab_diff", pa.string()),
            pa.field("modified_timdex_fields", pa.list_(pa.string())),
            pa.field("has_diff", pa.string()),
            pa.field("a_or_b_missing", pa.int32()),
            *metrics_columns,  # type: ignore[arg-type]
        )
    )

    # write records to records dataset
    records_dataset_path = str(Path(run_directory) / "records")
    write_to_dataset(
        get_final_records_iter(
            diffs_dataset_path, metrics_dataset_path, metrics_timdex_field_columns
        ),
        base_dir=records_dataset_path,
        schema=final_records_dataset_schema,
        partition_columns=["source", "has_diff"],
    )

    # initialize duckdb database file for future use
    duckdb_filepath = Path(run_directory) / "run.duckdb"
    create_duckdb_database_file(duckdb_filepath, records_dataset_path)
    update_or_create_run_json(run_directory, {"duckdb_filepath": str(duckdb_filepath)})

    return records_dataset_path


def get_final_records_iter(
    diffs_dataset_path: str,
    metrics_dataset_path: str,
    metrics_timdex_field_columns: list[str],
) -> Generator[pa.RecordBatch, None, None]:

    with duckdb.connect(":memory:") as conn:

        # create views for diffs and metrics datasets to join
        conn.execute(
            f"""
            create view diffs as
            select * from read_parquet(
                '{diffs_dataset_path}/**/*.parquet',
                hive_partitioning=true
            )
            """
        )
        conn.execute(
            f"""
            create view metrics as
            select * from read_parquet(
                '{metrics_dataset_path}/**/*.parquet',
                hive_partitioning=true
            )
            """
        )

        # prepare select columns
        select_columns = ",".join(
            [
                "d.timdex_record_id",
                "d.source",
                "d.record_a",
                "d.record_b",
                "d.ab_diff",
                "d.modified_timdex_fields",
                "d.has_diff",
                *[f"m.{name}" for name in metrics_timdex_field_columns],
            ]
        )

        results = conn.execute(
            f"""
            select {select_columns}
            from diffs d
            inner join metrics m on m.timdex_record_id = d.timdex_record_id
            """
        ).fetch_record_batch(READ_BATCH_SIZE)

        count = 0
        while True:
            try:
                count += 1
                logger.info(f"Yielding final records dataset batch: {count}")
                yield results.read_next_batch()
            except StopIteration:
                break


def create_duckdb_database_file(
    duckdb_filepath: str | Path, records_dataset_path: str
) -> None:
    """Create a DuckDB database file with views associated with records dataset.

    This DuckDB database file will contain only views or very small tables, taking up
    little space on disk.  These views will provided as a convenience for the webapp and
    other contexts to query the records dataset.
    """
    logger.info("creating duckdb database file")
    with duckdb.connect(duckdb_filepath) as conn:

        # create records dataset view
        parquet_glob_pattern = f"{records_dataset_path}/**/*.parquet"
        conn.execute(
            f"""
            create view records as
            select *
            from read_parquet('{parquet_glob_pattern}', hive_partitioning=true)
            """
        )
