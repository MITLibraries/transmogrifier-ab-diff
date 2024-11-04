import concurrent.futures
import json
import logging
import time
from collections.abc import Generator
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
from deepdiff import DeepDiff

from abdiff.core.utils import update_or_create_run_json, write_to_dataset

logger = logging.getLogger(__name__)

READ_BATCH_SIZE = 10_000
WRITE_MAX_ROWS_PER_FILE = 100_000
MAX_PARALLEL_WORKERS = 6

DIFFS_DATASET_OUTPUT_SCHEMA = pa.schema(
    (
        pa.field("timdex_record_id", pa.string()),
        pa.field("source", pa.string()),
        pa.field("record_a", pa.binary()),
        pa.field("record_b", pa.binary()),
        pa.field("ab_diff", pa.string()),
        pa.field("modified_timdex_fields", pa.list_(pa.string())),
        pa.field("has_diff", pa.string()),
    )
)


def calc_ab_diffs(run_directory: str, collated_dataset_path: str) -> str:
    """Create parquet dataset of records with A/B diff included."""
    start_time = time.time()

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

    update_or_create_run_json(run_directory, {"diffs_dataset_path": str(diffs_dataset)})

    logger.info(f"calc_ab_diffs complete, elapsed: {time.time()-start_time}")
    return str(diffs_dataset)


def process_batch(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Parallel worker for calculating record diffs for a batch.

    The pyarrow RecordBatch is converted into a pandas dataframe, a diff is calculated via
    DeepDiff for each record in the batch, and this is converted back to a pyarrow
    RecordBatch for returning.
    """
    df = batch.to_pandas()  # noqa: PD901
    diff_results = df.apply(
        lambda row: calc_record_diff(row["record_a"], row["record_b"]), axis=1
    )
    df["ab_diff"] = diff_results.apply(lambda x: x[0])
    df["modified_timdex_fields"] = diff_results.apply(
        lambda x: list(x[1]) if x[1] else []
    )
    df["has_diff"] = diff_results.apply(lambda x: x[2])
    return pa.RecordBatch.from_pandas(df)  # type: ignore[attr-defined]


def get_diffed_batches_iter(
    collated_dataset: ds.Dataset,
    batch_size: int = READ_BATCH_SIZE,
    max_parallel_processes: int = MAX_PARALLEL_WORKERS,
) -> Generator[pa.RecordBatch, None, None]:
    """Yield pyarrow record batches with diff calculated for each record.

    This work is performed in parallel, leveraging CPU cores to calculate the diffs and
    yield batches for writing to the "diffs" dataset.
    """
    batches_iter = collated_dataset.to_batches(batch_size=batch_size)

    with concurrent.futures.ProcessPoolExecutor(
        max_workers=max_parallel_processes + 1
    ) as executor:
        pending_futures = []
        for batch_count, batch in enumerate(batches_iter):
            logger.info(f"Submitting batch {batch_count} for processing")
            future = executor.submit(process_batch, batch)
            pending_futures.append((batch_count, future))

            if len(pending_futures) >= max_parallel_processes:
                idx, completed_future = pending_futures.pop(0)
                result = completed_future.result()
                logger.info(f"Yielding diffed batch: {idx}")
                yield result

        for idx, future in pending_futures:
            result = future.result()
            logger.info(f"Yielding diffed batch: {idx}")
            yield result


def calc_record_diff(
    record_a: str | bytes | dict | None,
    record_b: str | bytes | dict | None,
    *,
    ignore_order: bool = True,
    report_repetition: bool = True,
) -> tuple[str | None, list[str] | None, bool]:
    """Calculate diff from two JSON byte strings.

    The DeepDiff library has the property 'affected_root_keys' on the produced diff object
    that is very useful for our purposes.  At this time, we simply want to know if
    anything about a particular root level TIMDEX field (e.g. 'dates' or 'title') has
    changed which this method provides explicitly.  We also serialize the full diff to
    JSON via the to_json() method for storage and possible further analysis.

    This method returns a tuple:
        - ab_diff: [str] - full diff as JSON
        - modified_timdex_fields: list[str] - list of modified root keys (TIMDEX fields)
        - has_diff: bool - True/False if any diff present
    """
    if record_a is None or record_b is None:
        return None, None, False

    diff = DeepDiff(
        json.loads(record_a) if isinstance(record_a, str | bytes) else record_a,
        json.loads(record_b) if isinstance(record_b, str | bytes) else record_b,
        ignore_order=ignore_order,
        report_repetition=report_repetition,
    )

    ab_diff = diff.to_json()
    modified_timdex_fields = diff.affected_root_keys
    has_diff = bool(modified_timdex_fields)

    return ab_diff, modified_timdex_fields, has_diff
