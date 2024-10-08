import logging
import time
from collections.abc import Generator
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
from jsondiff import diff

from abdiff.core.utils import update_or_create_run_json, write_to_dataset

logger = logging.getLogger(__name__)

READ_BATCH_SIZE = 1_000
WRITE_MAX_ROW_GROUP_SIZE = 1_000
WRITE_MAX_ROWS_PER_FILE = 100_000

DIFFS_DATASET_OUTPUT_SCHEMA = pa.schema(
    (
        pa.field("timdex_record_id", pa.string()),
        pa.field("source", pa.string()),
        pa.field("record_a", pa.binary()),
        pa.field("record_b", pa.binary()),
        pa.field("ab_diff", pa.string()),
        pa.field("has_diff", pa.string()),
    )
)


def calc_ab_diffs(run_directory: str, collated_dataset_path: str) -> str:
    """Create parquet dataset of records with A/B diff included."""
    t0 = time.time()

    diffs_dataset = Path(run_directory) / "diffs"

    collated_dataset = ds.dataset(collated_dataset_path, partitioning="hive")
    batches_iter = get_diffed_batches_iter(collated_dataset)

    written_files = write_to_dataset(
        batches_iter,
        schema=DIFFS_DATASET_OUTPUT_SCHEMA,
        base_dir=diffs_dataset,
        partition_columns=["has_diff"],
    )
    logger.info(f"wrote {len(written_files)} parquet files to diffs dataset")

    update_or_create_run_json(run_directory, {"diffs_dataset": str(diffs_dataset)})

    logger.info(f"calc_ab_diffs complete, elapsed: {time.time()-t0}")
    return str(diffs_dataset)


def get_diffed_batches_iter(
    collated_dataset: ds.Dataset,
    batch_size: int = READ_BATCH_SIZE,
) -> Generator[pa.RecordBatch, None, None]:
    """Yield pyarrow record batches with diff calculated for records in batch."""
    batches_iter = collated_dataset.to_batches(batch_size=batch_size)
    for i, batch in enumerate(batches_iter):
        logger.info(f"Calculating AB diff for batch: {i}")

        # convert batch to pandas dataframe and calc values for new columns
        df = batch.to_pandas()  # noqa: PD901
        df["ab_diff"] = df.apply(
            lambda row: calc_record_diff(row["record_a"], row["record_b"]), axis=1
        )
        df["has_diff"] = df["ab_diff"].apply(lambda diff_value: diff_value != "{}")

        yield pa.RecordBatch.from_pandas(df)  # type: ignore[attr-defined]


def calc_record_diff(record_a: bytes | None, record_b: bytes | None) -> str | None:
    """Calculate symmetric diff from two JSON strings."""
    if record_a is None or record_b is None:
        return None

    return diff(
        record_a.decode(),
        record_b.decode(),
        syntax="symmetric",
        load=True,
        dump=True,
    )
