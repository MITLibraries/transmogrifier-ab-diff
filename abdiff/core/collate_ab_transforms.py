# ruff: noqa: TRY003

import itertools
import json
import logging
import re
import tempfile
from collections.abc import Generator
from pathlib import Path

import duckdb
import ijson
import pandas as pd
import pyarrow as pa

from abdiff.core.exceptions import OutputValidationError
from abdiff.core.utils import parse_timdex_filename, write_to_dataset

logger = logging.getLogger(__name__)

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
           partitioned by the transformed file name.
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

    # build temporary collated dataset
    joined_written_files = write_to_dataset(
        get_joined_batches_iter(transformed_dataset_path.name),
        base_dir=joined_dataset_path.name,
        schema=COLLATED_DATASET_SCHEMA,
    )
    logger.info(f"Wrote {len(joined_written_files)} parquet file(s) to collated dataset")

    # build final deduped and collated dataset
    deduped_written_files = write_to_dataset(
        get_deduped_batches_iter(joined_dataset_path.name),
        base_dir=collated_dataset_path,
        schema=COLLATED_DATASET_SCHEMA,
    )
    logger.info(
        f"Wrote {len(deduped_written_files)} parquet file(s) to deduped collated dataset"
    )

    validate_output(collated_dataset_path)

    # ensure temporary artifacts removed
    transformed_dataset_path.cleanup()
    joined_dataset_path.cleanup()

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
            for record in ijson.items(file, "item"):
                yield {
                    **base_record,
                    "timdex_record_id": record["timdex_record_id"],
                    "record": json.dumps(record).encode(),
                }

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
    """
    for transformed_files in ab_transformed_file_lists:
        for transformed_file in transformed_files:
            record_iter = get_transformed_records_iter(
                transformed_file=str(Path(run_directory) / transformed_file)
            )
            for record_batch in itertools.batched(record_iter, READ_BATCH_SIZE):
                yield pa.RecordBatch.from_pylist(list(record_batch))


def get_joined_batches_iter(dataset_directory: str) -> Generator[pa.RecordBatch]:
    """Yield pyarrow.RecordBatch objects of joined TIMDEX A/B records.

    This function uses DuckDB to query the Parquet dataset of transformed
    TIMDEX record dictionaries. It's worth noting that this Parquet dataset
    is stored in a tempfile.TemporaryDirectory that gets deleted after the
    function exits.

    The following steps are performed in sequential order:
        1. A list of DISTINCT transformed filenames (without the file extension)
           is retrieved.

        Steps 2-4 are performed for each transformed file.

        2. Using common table expressions (CTEs) execute a query that performs
           the following:
           - Create a CTE called 'transformed_file' that contains all the records
             where the partition transformed_file_name=the name of the transformed file.
           - Create a CTE called 'a' that contains all the records from CTE
             'transformed_file' where the column version='a'.
           - Create a CTE called 'b' that contains all the records from CTE
             'transformed_file' where the column version='b'.
           - Join the 'a' and 'b' CTEs using the column timdex_record_id.

        3. Read results from the query one batch at a time using fetch_record_batch().
           Note: This will return the results as a pyarrow.RecordBatchReader.

        4. Yield batches of the joined records until the pyarrow.RecordBatchReader
           is empty.

    The returned generator can be passed to abdiff.core.utils.write_to_dataset()
    to perform batch writes of joined TIMDEX A/B records to a Parquet dataset

    Args:
        dataset_directory: The root directory of the Parquet dataset of TIMDEX records
            (i.e., the tempfile.TemporaryDirectory).
    """
    with duckdb.connect(":memory:") as con:
        transformed_file_names = con.execute(
            """
            SELECT DISTINCT transformed_file_name FROM
            read_parquet($transformed_parquet_glob, hive_partitioning=true)
            """,
            {"transformed_parquet_glob": f"{dataset_directory}/**/*.parquet"},
        ).fetch_df()["transformed_file_name"]

        for transformed_file in transformed_file_names:
            results = con.execute(
                """
                WITH
                    transformed_file AS (
                        SELECT * FROM
                        read_parquet(
                            $transformed_parquet_glob,
                            hive_partitioning=true
                        )
                        WHERE transformed_file_name=$transformed_file_name
                    ),
                    a AS (SELECT * FROM transformed_file WHERE version='a'),
                    b AS (SELECT * FROM transformed_file WHERE version='b')
                SELECT
                    COALESCE(a.timdex_record_id, b.timdex_record_id) timdex_record_id,
                    COALESCE(a.source, b.source) source,
                    COALESCE(a.run_date, b.run_date) run_date,
                    COALESCE(a.run_type, b.run_type) run_type,
                    COALESCE(a.action, b.action) "action",
                    a.record as record_a,
                    b.record as record_b
                FROM a
                FULL OUTER JOIN b USING (timdex_record_id)
                """,
                {
                    "transformed_parquet_glob": f"{dataset_directory}/**/*.parquet",
                    "transformed_file_name": transformed_file,
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

    This function writes the final dataset by deduping records from the temporary collated
    dataset, given the following logic:
        - use the MOST RECENT record based on 'run_date'
        - if the MOST RECENT record is action='delete', then omit record entirely

    The same mechanism is used by get_joined_batches_iter() to perform a DuckDB query then
    stream write batches to a parquet dataset.
    """
    with duckdb.connect(":memory:") as con:

        results = con.execute(
            """
            WITH collated as (
              select * from read_parquet($collated_parquet_glob, hive_partitioning=true)
            ),
            latest_records AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY timdex_record_id
                        ORDER BY run_date DESC
                    ) AS rn
                FROM collated
            ),
            deduped_records AS (
                SELECT *
                FROM latest_records
                WHERE rn = 1 AND action != 'delete'
            )
            SELECT
                timdex_record_id,
                source,
                run_date,
                run_type,
                action,
                record_a,
                record_b
            FROM deduped_records;
            """,
            {
                "collated_parquet_glob": f"{dataset_directory}/**/*.parquet",
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
