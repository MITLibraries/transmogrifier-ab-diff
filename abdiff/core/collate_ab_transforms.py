import itertools
import json
import logging
import re
import tempfile
from collections.abc import Generator
from pathlib import Path

import duckdb
import ijson
import pyarrow as pa

from abdiff.core.utils import write_to_dataset

logger = logging.getLogger(__name__)

READ_BATCH_SIZE = 1_000
TRANSFORMED_DATASET_SCHEMA = pa.schema(
    [
        pa.field("timdex_record_id", pa.string()),
        pa.field("source", pa.string()),
        pa.field("record", pa.binary()),
        pa.field("version", pa.string()),
        pa.field("transformed_file_name", pa.string()),
    ]
)
JOINED_DATASET_SCHEMA = pa.schema(
    [
        pa.field("timdex_record_id", pa.string()),
        pa.field("source", pa.string()),
        pa.field("record_a", pa.binary()),
        pa.field("record_b", pa.binary()),
    ]
)


def collate_ab_transforms(
    run_directory: str, transformed_files: tuple[list[str], ...]
) -> str:
    """Collates A/B transformed files into a Parquet dataset.

    This process can be summarized into two (2) important steps:
        1. Write all transformed JSON records into a temporary Parquet dataset
           partitioned by the transformed file name.
        2. For every transformed file, use DuckDB to join A/B Parquet tables
           using the TIMDEX record ID and write joined records to a Parquet dataset.

    This function (and its subfunctions) uses DuckDB, generators, and batching to
    write records to Parquet datasets in a memory-efficient manner.
    """
    transformed_dataset_path = tempfile.TemporaryDirectory()
    collated_dataset_path = str(Path(run_directory) / "collated")

    transformed_written_files = write_to_dataset(
        get_transformed_batches_iter(run_directory, transformed_files),
        schema=TRANSFORMED_DATASET_SCHEMA,
        base_dir=transformed_dataset_path.name,
        partition_columns=["transformed_file_name"],
    )
    logger.info(
        f"Wrote {len(transformed_written_files)} parquet file(s) to transformed dataset"
    )

    joined_written_files = write_to_dataset(
        get_joined_batches_iter(transformed_dataset_path.name),
        base_dir=collated_dataset_path,
        schema=JOINED_DATASET_SCHEMA,
    )
    logger.info(f"Wrote {len(joined_written_files)} parquet file(s) to collated dataset")
    return collated_dataset_path


def yield_records(transformed_file: str | Path) -> Generator:
    """Yields data for every TIMDEX record in a transformed file.

    This function uses ijson to yield records from a JSON stream
    (i.e., the transformed file) one at a time. A generator is returned,
    yielding a dictionary that contains the following:

    * timdex_record_id: The TIMDEX record ID.
    * source: The shorthand name of the source as denoted in by Transmogrifier
      (see https://github.com/MITLibraries/transmogrifier/blob/main/transmogrifier/config.py).
    * record: The TIMDEX record serialized to a JSON string then encoded to bytes.
    * version: The version of the transform, parsed from the absolute filepath to a
      transformed file.
    * transformed_file_name: The name of the transformed file, excluding file extension.
    """
    version, transformed_file_name = parse_parquet_details_from_transformed_file(
        str(transformed_file)
    )
    with open(transformed_file) as file:
        records = ijson.items(file, "item")
        for record in records:
            yield {
                "timdex_record_id": record["timdex_record_id"],
                "source": record["source"],
                "record": json.dumps(record).encode(),
                "version": version,
                "transformed_file_name": transformed_file_name,
            }


def get_transformed_batches_iter(
    run_directory: str, transformed_files: tuple[list[str], ...]
) -> Generator:
    """Yield pyarrow.RecordBatch objects of TIMDEX records.

    This function will iterate over the A/B lists in 'transformed_files',
    calling yield_records() to fetch dictionaries describing
    TIMDEX records and compiling the dictionaries into a
    pyarrow.RecordBatch. The size of a batch is set by the
    'READ_BATCH_SIZE' global variable.

    The function returns a generator, yielding batches of the dictionaries
    from yield_records(). The returned generator can be passed to
    abdiff.core.utils.write_to_dataset() to perform batch writes to
    a Parquet dataset.
    """
    for transformed_files_list in transformed_files:
        for transformed_file in transformed_files_list:
            record_iter = yield_records(
                transformed_file=Path(run_directory) / transformed_file
            )
            for record_batch in itertools.batched(record_iter, READ_BATCH_SIZE):
                yield pa.RecordBatch.from_pylist(list(record_batch))


def get_joined_batches_iter(dataset_directory: str) -> Generator:
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
    with duckdb.connect() as con:
        transformed_file_names = con.execute(
            f"""
            SELECT DISTINCT transformed_file_name FROM
            read_parquet('{dataset_directory}/**/*.parquet', hive_partitioning=true)
            """
        ).fetchall()[0]

        for transformed_file in transformed_file_names:
            results = con.execute(
                f"""
                WITH
                    transformed_file AS (
                        SELECT * FROM
                        read_parquet(
                            '{dataset_directory}/**/*.parquet',
                            hive_partitioning=true
                        )
                        WHERE transformed_file_name='{transformed_file}'
                    ),
                    a AS (SELECT * FROM transformed_file WHERE version='a'),
                    b AS (SELECT * FROM transformed_file WHERE version='b')
                SELECT
                    a.timdex_record_id,
                    a.source,
                    a.record as record_a,
                    b.record as record_b
                FROM a
                FULL OUTER JOIN b USING (timdex_record_id)
                """
            ).fetch_record_batch(READ_BATCH_SIZE)

            while True:
                try:
                    yield results.read_next_batch()
                except StopIteration:
                    logger.info("Already fetched all batches.")
                    break


def parse_parquet_details_from_transformed_file(transformed_file: str) -> tuple[str, ...]:
    """Parse parquet details from the absolute path of a transformed file.

    This will retrieve the transmogrifier image version ('a' or 'b') and
    the transformed filename.
    """
    match_result = re.match(
        r".*transformed\/(.*)\/(.*).json",
        transformed_file,
    )
    if not match_result:
        raise ValueError(  # noqa: TRY003
            f"Transformed filename is invalid: {transformed_file}."
        )
    version, filename = match_result.groups()
    return version, filename
