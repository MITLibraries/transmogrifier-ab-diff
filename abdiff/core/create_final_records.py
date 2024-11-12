import logging
from collections.abc import Generator
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.dataset as ds

from abdiff.config import Config
from abdiff.core.utils import load_dataset, write_to_dataset

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
    """
    logger.info("Creating final records dataset from 'diffs' and 'metrics' datasets.")

    diffs_dataset = load_dataset(diffs_dataset_path)
    metrics_dataset = load_dataset(metrics_dataset_path)

    # get list of unique columns from metrics dataset, and create final dataset schema
    metrics_timdex_field_columns = [
        name
        for name in metrics_dataset.schema.names
        if name not in diffs_dataset.schema.names
    ]
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
            *metrics_columns,  # type: ignore[arg-type]
        )
    )

    records_dataset_path = str(Path(run_directory) / "records")
    write_to_dataset(
        get_final_records_iter(
            diffs_dataset, metrics_dataset, metrics_timdex_field_columns
        ),
        base_dir=records_dataset_path,
        schema=final_records_dataset_schema,
        partition_columns=["source", "has_diff"],
    )

    return records_dataset_path


def get_final_records_iter(
    diffs_dataset: ds.Dataset,
    metrics_dataset: ds.Dataset,
    metrics_timdex_field_columns: list[str],
) -> Generator[pa.RecordBatch, None, None]:

    with duckdb.connect(":memory:") as conn:

        # register datasets in DuckDB for use
        conn.register("diffs", diffs_dataset.to_table())
        conn.register("metrics", metrics_dataset.to_table())

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