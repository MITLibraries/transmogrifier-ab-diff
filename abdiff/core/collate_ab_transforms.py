# ruff: noqa: TRY003

import logging
import re
import shutil
import tempfile
from collections.abc import Generator
from pathlib import Path

import duckdb
import pyarrow as pa

from abdiff.config import Config
from abdiff.core.exceptions import OutputValidationError
from abdiff.core.utils import write_to_dataset

logger = logging.getLogger(__name__)

CONFIG = Config()

READ_BATCH_SIZE = 1_000
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


def collate_ab_transforms(run_directory: str, ab_transformed_datasets: list[str]) -> str:
    """Collates A/B transformed records into a Parquet dataset."""
    joined_dataset_path = tempfile.TemporaryDirectory()
    collated_dataset_path = str(Path(run_directory) / "collated")

    # build temporary dataset of joined A/B records
    joined_written_files = write_to_dataset(
        get_joined_batches_iter(ab_transformed_datasets),
        base_dir=joined_dataset_path.name,
        schema=COLLATED_DATASET_SCHEMA,
    )
    logger.info(f"Wrote {len(joined_written_files)} parquet file(s) to collated dataset")

    # remove transformed datasets
    if not CONFIG.preserve_artifacts:
        shutil.rmtree(Path(run_directory) / "transformed")
        logger.info("/transformed dataset directories removed completely.")

    # build final dataset
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


def get_joined_batches_iter(
    ab_transformed_datasets: list[str],
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
        ab_transformed_datasets: Location of Transmogrifier A and B output datasets.
    """
    dataset_a, dataset_b = ab_transformed_datasets

    with duckdb.connect(":memory:") as con:
        results = con.execute(
            """
            WITH
                a AS (
                    SELECT
                        timdex_record_id,
                        source,
                        run_date,
                        run_type,
                        action,
                        transformed_record
                    FROM
                    read_parquet(
                        $a_glob,
                        hive_partitioning=true
                    )
                ),
                b AS (
                    SELECT
                        timdex_record_id,
                        source,
                        run_date,
                        run_type,
                        action,
                        transformed_record
                    FROM
                    read_parquet(
                        $b_glob,
                        hive_partitioning=true
                    )
                )
            SELECT
                uuid() as abdiff_record_id,
                COALESCE(a.timdex_record_id, b.timdex_record_id) as timdex_record_id,
                COALESCE(a.source, b.source) as source,
                COALESCE(a.run_date, b.run_date) as run_date,
                COALESCE(a.run_type, b.run_type) as run_type,
                COALESCE(a.action, b.action) AS action,
                a.transformed_record AS record_a,
                b.transformed_record AS record_b
            FROM a
            FULL OUTER JOIN b USING (timdex_record_id)
            """,
            {
                "a_glob": f"{dataset_a}/dataset/**/*.parquet",
                "b_glob": f"{dataset_b}/dataset/**/*.parquet",
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
