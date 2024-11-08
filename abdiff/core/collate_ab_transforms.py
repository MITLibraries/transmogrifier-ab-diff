# ruff: noqa: TRY003

import glob
import itertools
import json
import logging
import os
import re
import shutil
import tempfile
from collections.abc import Generator
from pathlib import Path

import duckdb
import ijson
import pandas as pd
import pyarrow as pa

from abdiff.config import Config
from abdiff.core.exceptions import OutputValidationError
from abdiff.core.utils import parse_timdex_filename, write_to_dataset

logger = logging.getLogger(__name__)

CONFIG = Config()

READ_BATCH_SIZE = 1_000
TRANSFORMED_DATASET_SCHEMA = pa.schema(
    (
        pa.field("timdex_record_id", pa.string()),
        pa.field("source", pa.string()),
        pa.field("run_date", pa.date32()),
        pa.field("run_type", pa.string()),
        pa.field("action", pa.string()),
        pa.field("record", pa.binary()),
        pa.field("version", pa.string()),
        pa.field("transformed_file_name", pa.string()),
    )
)
COLLATED_DATASET_SCHEMA = pa.schema(
    (
        pa.field("abdiff_record_id", pa.string()),
        pa.field("timdex_record_id", pa.string()),
        pa.field("source", pa.string()),
        pa.field("run_date", pa.date32()),
        pa.field("run_type", pa.string()),
        pa.field("action", pa.string()),
        pa.field("record_a", pa.binary()),
        pa.field("record_b", pa.binary()),
    )
)


def collate_ab_transforms(
    run_directory: str, ab_transformed_file_lists: tuple[list[str], ...]
) -> str:
    """Collates A/B transformed files into a Parquet dataset.

    This process can be summarized into two (3) important steps:
        1. Write all transformed JSON records into a temporary Parquet dataset
           partitioned by the transformed file name.  As this process works, transformed
           files that are no longer needed are removed to free storage space.
        2. For every transformed file, use DuckDB to join A/B Parquet tables
           using the TIMDEX record ID and write joined records to a Parquet dataset.
        3. Dedupe joined records to ensure that only the most recent, not "deleted"
           timdex_record_id is present in final output.
    """
    transformed_dataset_path = tempfile.TemporaryDirectory()
    joined_dataset_path = tempfile.TemporaryDirectory()
    collated_dataset_path = str(Path(run_directory) / "collated")

    # build temporary transformed dataset
    transformed_written_files = write_to_dataset(
        get_transformed_batches_iter(run_directory, ab_transformed_file_lists),
        schema=TRANSFORMED_DATASET_SCHEMA,
        base_dir=transformed_dataset_path.name,
        partition_columns=["transformed_file_name"],
    )
    logger.info(
        f"Wrote {len(transformed_written_files)} parquet file(s) to transformed dataset"
    )

    # remove transformed directory entirely
    if not CONFIG.preserve_artifacts:
        shutil.rmtree(Path(run_directory) / "transformed")
        logger.info("/transformed directory removed completely.")

    # build temporary collated dataset
    joined_written_files = write_to_dataset(
        get_joined_batches_iter(transformed_dataset_path.name),
        base_dir=joined_dataset_path.name,
        schema=COLLATED_DATASET_SCHEMA,
    )
    logger.info(f"Wrote {len(joined_written_files)} parquet file(s) to collated dataset")
    transformed_dataset_path.cleanup()
    logger.info("Temporary transformed files dataset removed.")

    # build final deduped and collated dataset
    deduped_written_files = write_to_dataset(
        get_deduped_batches_iter(joined_dataset_path.name),
        base_dir=collated_dataset_path,
        schema=COLLATED_DATASET_SCHEMA,
    )
    logger.info(
        f"Wrote {len(deduped_written_files)} parquet file(s) to deduped collated dataset"
    )
    joined_dataset_path.cleanup()
    logger.info("Temporary joined records dataset removed.")

    validate_output(collated_dataset_path)

    return collated_dataset_path


def get_transformed_records_iter(
    transformed_file: str,
) -> Generator[dict[str, str | bytes | None]]:
    """Yields data for every TIMDEX record in a transformed file.

    This function uses ijson to yield records from a JSON stream
    (i.e., the transformed file) one at a time. A generator is returned,
    yielding a dictionary that contains the following:

    * timdex_record_id: The TIMDEX record ID.
    * source: The shorthand name of the source as denoted in by Transmogrifier
    * run_date: Run date from TIMDEX ETL
    * run_type: "full" or "daily"
    * action: "index" or "delete"
    * record: The TIMDEX record serialized to a JSON string then encoded to bytes.
    * version: The version of the transform, parsed from the absolute filepath to a
      transformed file.
    * transformed_file_name: The name of the transformed file, excluding file extension.
    """
    version = get_transform_version(transformed_file)
    filename_details = parse_timdex_filename(transformed_file)

    base_record = {
        "source": filename_details["source"],
        "run_date": filename_details["run-date"],
        "run_type": filename_details["run-type"],
        "action": filename_details["action"],
        "version": version,
        "transformed_file_name": transformed_file.split("/")[-1],
    }

    # handle JSON files with records to index
    if transformed_file.endswith(".json"):
        with open(transformed_file, "rb") as file:
            try:
                for record in ijson.items(file, "item"):
                    yield {
                        **base_record,
                        "timdex_record_id": record["timdex_record_id"],
                        "record": json.dumps(record).encode(),
                    }
            except:  # noqa: E722
                logger.exception(f"Could not yield records from file: {transformed_file}")

    # handle TXT files with records to delete
    else:
        deleted_records_df = pd.read_csv(transformed_file, header=None)
        for row in deleted_records_df.itertuples():
            yield {
                **base_record,
                "timdex_record_id": row[1],
                "record": None,
            }


def get_transformed_batches_iter(
    run_directory: str, ab_transformed_file_lists: tuple[list[str], ...]
) -> Generator[pa.RecordBatch]:
    """Yield pyarrow.RecordBatch objects of TIMDEX records.

    This function will iterate over the A/B lists in 'transformed_files',
    calling get_transformed_records_iter() to fetch dictionaries describing
    TIMDEX records and compiling the dictionaries into a
    pyarrow.RecordBatch. The size of a batch is set by the
    'READ_BATCH_SIZE' global variable.

    The function returns a generator, yielding batches of the dictionaries
    from get_transformed_records_iter(). The returned generator can be passed to
    abdiff.core.utils.write_to_dataset() to perform batch writes to
    a Parquet dataset.

    After yielding records, for both A and B versions, the transformed file is removed.
    """
    count = 0
    total_transformed_files = len(ab_transformed_file_lists[0]) + len(
        ab_transformed_file_lists[1]
    )

    for transformed_files in ab_transformed_file_lists:
        for transformed_file in transformed_files:
            count += 1
            logger.debug(
                f"Yielding records from file {count} / {total_transformed_files}"
                f": {transformed_file}"
            )
            transformed_filepath = str(Path(run_directory) / transformed_file)
            record_iter = get_transformed_records_iter(
                transformed_file=transformed_filepath
            )
            for record_batch in itertools.batched(record_iter, READ_BATCH_SIZE):
                yield pa.RecordBatch.from_pylist(list(record_batch))

            # remove transformed file no longer needed
            if not CONFIG.preserve_artifacts:
                logger.info(f"removing original transformed file: {transformed_filepath}")
                os.remove(transformed_filepath)


def get_joined_batches_iter(
    dataset_directory: str,
) -> Generator[pa.RecordBatch, None, None]:
    """Yield pyarrow.RecordBatch objects of joined TIMDEX A/B records.

    The previous step created a parquet dataset where each A and B version of a record,
    from a single input file, was a distinct row in the dataset.  This function joins the
    A and B versions of the same intellectual record as a single row, by limiting to the
    transformed file.

    For performance reasons, DuckDB is provided with ONLY the parquet files that are
    associated with that transformed file, to prevent it from scanning all files in the
    dataset (this was memory safe, but somewhat slow given the potentially large number
    of transformed files).

    As records are joined, an "abdiff_record_id" UUID is minted to unambiguously reference
    that particular row, which is helpful later during deduping.

    As the records are joined, record batches are yielded which get written to a new,
    temporary "joined" dataset on disk.

    Args:
        dataset_directory: The root directory of the Parquet dataset of TIMDEX records
            (i.e., the tempfile.TemporaryDirectory).
    """
    partition_paths = glob.glob(f"{dataset_directory}/transformed_file_name=*")
    transformed_file_names = [os.path.basename(p).split("=")[1] for p in partition_paths]

    with duckdb.connect(":memory:") as con:
        for i, transformed_file in enumerate(transformed_file_names):
            logger.debug(
                f"Joining records batch {i+1}/{len(transformed_file_names)}: "
                f"{transformed_file}"
            )

            transformed_file_parquet_glob = (
                f"{dataset_directory}/transformed_file_name={transformed_file}/*.parquet"
            )

            results = con.execute(
                """
                WITH
                    transformed_file AS (
                        SELECT * FROM
                        read_parquet(
                            $transformed_file_parquet_glob,
                            hive_partitioning=true
                        )
                    ),
                    a AS (SELECT * FROM transformed_file WHERE version='a'),
                    b AS (SELECT * FROM transformed_file WHERE version='b')
                SELECT
                    uuid() as abdiff_record_id,
                    a.timdex_record_id,
                    a.source,
                    a.run_date,
                    a.run_type,
                    COALESCE(a.action, b.action) AS action,
                    a.record AS record_a,
                    b.record AS record_b
                FROM a
                FULL OUTER JOIN b USING (timdex_record_id)
                """,
                {
                    "transformed_file_parquet_glob": transformed_file_parquet_glob,
                },
            ).fetch_record_batch(READ_BATCH_SIZE)

            while True:
                try:
                    yield results.read_next_batch()
                except StopIteration:
                    break


def get_deduped_batches_iter(dataset_directory: str) -> Generator[pa.RecordBatch]:
    """Yield pyarrow.RecordBatch objects of deduped rows from the joined dataset.

    ABDiff should be able to handle many input files, where a single timdex_record_id may
    be duplicated across multiple files ("full" vs "daily" runs, incrementing date runs,
    etc.)

    This function writes the final dataset by deduping records from the temporary joined
    dataset, given the following logic:
        - use the MOST RECENT record based on 'run_date'
        - if the MOST RECENT record is action='delete', then omit record entirely

    The same mechanism from get_joined_batches_iter() to perform a DuckDB query then yield
    batches of records that are written to the final "collated" dataset is used.
    """
    with duckdb.connect(":memory:") as con:

        results = con.execute(
            """
            WITH joined as (
                select * from read_parquet($joined_parquet_glob, hive_partitioning=true)
            ),
            latest_records AS (
                SELECT
                    abdiff_record_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY timdex_record_id
                        /*
                        This ordering is important:
                            1. order by run date, ensuring most recent is first
                            2. order by run-type, where "daily" sorts before "full"
                            3. order by action, where "delete" takes precedence over
                               "index"
                        This ordering coupled with the ROW_NUMBER(), ensures we get only
                        a single and most recent timdex_record_id in the dataset.
                        */
                        ORDER BY
                            run_date desc,
                            run_type,
                            action
                    ) AS rn
                FROM joined
            ),
            deduped_records AS (
                SELECT *
                FROM latest_records
                WHERE rn = 1 AND action != 'delete'
            )
            SELECT
                j.abdiff_record_id,
                j.timdex_record_id,
                j.source,
                j.run_date,
                j.run_type,
                j.action,
                j.record_a,
                j.record_b
            FROM joined j
            inner join deduped_records dr on dr.abdiff_record_id = j.abdiff_record_id
            """,
            {
                "joined_parquet_glob": f"{dataset_directory}/**/*.parquet",
            },
        ).fetch_record_batch(READ_BATCH_SIZE)

        while True:
            try:
                yield results.read_next_batch()
            except StopIteration:
                break  # pragma: nocover


def validate_output(dataset_path: str) -> None:
    """Validate the output of collate_ab_transforms.

    This function checks whether the collated dataset is empty
    and whether any or both 'record_a' or 'record_b' columns are
    totally empty.
    """

    def fetch_single_value(query: str) -> int:
        result = con.execute(query).fetchone()
        if result is None:
            raise RuntimeError(f"Query returned no results: {query}")  # pragma: nocover
        return int(result[0])

    with duckdb.connect(":memory:") as con:
        con.execute(
            f"""
            CREATE VIEW collated AS (
                SELECT * FROM read_parquet('{f"{dataset_path}/**/*.parquet"}')
            )
            """
        )

        # check if the table is empty
        record_count = fetch_single_value("SELECT COUNT(*) FROM collated")
        if record_count == 0:
            raise OutputValidationError(
                "The collated dataset does not contain any records."
            )

        # check if any of the 'record_*' columns are empty
        record_a_null_count = fetch_single_value(
            "SELECT COUNT(*) FROM collated WHERE record_a ISNULL"
        )
        record_b_null_count = fetch_single_value(
            "SELECT COUNT(*) FROM collated WHERE record_b ISNULL"
        )

        if record_count in {record_a_null_count, record_b_null_count}:
            raise OutputValidationError(
                "At least one or both record column(s) ['record_a', 'record_b'] "
                "in the collated dataset are empty."
            )

        # check that timdex_record_id column is unique
        non_unique_count = fetch_single_value(
            """
            SELECT COUNT(*)
            FROM (
                SELECT timdex_record_id
                FROM collated
                GROUP BY timdex_record_id
                HAVING COUNT(*) > 1
            ) as duplicates;
        """
        )
        if non_unique_count > 0:
            raise OutputValidationError(
                "The collated dataset contains duplicate 'timdex_record_id' records."
            )


def get_transform_version(transformed_filepath: str) -> str:
    """Get A/B transform version, either 'a' or 'b'."""
    match_result = re.match(r".*transformed\/(.*)\/.*", transformed_filepath)
    if not match_result:
        raise ValueError(f"Transformed filepath is invalid: {transformed_filepath}.")

    return match_result.groups()[0]
