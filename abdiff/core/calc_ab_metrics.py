# ruff: noqa: S608

import json
import logging
import os
import time
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
from duckdb.duckdb import DuckDBPyConnection

from abdiff.core.utils import load_dataset, update_or_create_run_json, write_to_dataset

logger = logging.getLogger(__name__)

NON_TIMDEX_FIELD_COLUMNS = ["timdex_record_id", "source", "has_diff"]


def calc_ab_metrics(
    run_directory: str,
    diffs_dataset_path: str,
) -> dict:

    os.makedirs(Path(run_directory) / "metrics", exist_ok=True)

    # build field diffs dataframe
    field_matrix_dataset_filepath = create_record_diff_matrix_dataset(
        run_directory, diffs_dataset_path
    )

    # calculate metrics data from sparse matrix
    metrics_data = calculate_metrics_data(field_matrix_dataset_filepath)

    # update run data with metrics
    update_or_create_run_json(
        run_directory=run_directory, new_data={"metrics": metrics_data}
    )

    return metrics_data


def create_record_diff_matrix_dataset(
    run_directory: str,
    diffs_dataset: str,
    batch_size: int = 1_000,
) -> str:
    """Create a boolean sparse matrix of modified fields for all records.

    This writes a single parquet file with rows for each record, and columns for each
    TIMDEX field, and a value of integer 1 if that field has a diff and 0 if not.  This
    provides a handy way to calculate aggregate metrics for a given field or source in
    later steps.  The column "has_diff" is also carried over from the diffs dataset to
    provide a single column to check if ANY of the field columns indicate a diff for a
    record row.

    This code momentarily creates a single dataframe in memory for all rows.  This is safe
    given the nature of the dataframe: there may be 10m rows, and potentially 20-30
    columns, but all values are either integer 1 or 0 representing a boolean, resulting in
    a very small dataset in memory despite a potentially high row count.
    """
    diffs_ds = load_dataset(diffs_dataset)

    batch_metrics_dfs = []
    for i, batch in enumerate(
        diffs_ds.to_batches(
            batch_size=batch_size,
            columns=["timdex_record_id", "source", "ab_diff", "has_diff"],
        )
    ):
        start_time = time.time()
        batch_df = batch.to_pandas()

        # parse diff JSON to dictionary for batch
        batch_df["ab_diff"] = batch_df["ab_diff"].apply(
            lambda diff_json: json.loads(diff_json)
        )

        batch_metrics = []
        for _, row in batch_df.iterrows():
            record_metrics = {
                "timdex_record_id": row["timdex_record_id"],
                "source": row["source"],
                "has_diff": 1 if row["has_diff"] == "true" else 0,
            }
            diff_data = row["ab_diff"]
            record_metrics.update(generate_field_diff_bools_for_record(diff_data))
            batch_metrics.append(record_metrics)

        # build dataframe for batch
        batch_metrics_df = pd.DataFrame(batch_metrics)
        batch_metrics_dfs.append(batch_metrics_df)
        logger.info(f"batch: {i+1}, elapsed: {time.time()-start_time}")

    # concatenate all dataframes into single dataframe for writing and replace None with 0
    metrics_df = pd.concat(batch_metrics_dfs)
    metrics_df = metrics_df.fillna(0)

    # write parquet dataset
    metrics_dataset = str(Path(run_directory) / "metrics")
    write_to_dataset(
        data=pa.Table.from_pandas(metrics_df, preserve_index=False),
        base_dir=metrics_dataset,
        partition_columns=["source"],
    )
    return metrics_dataset


def generate_field_diff_bools_for_record(diff_data: dict) -> dict:
    """Function to return dictionary of fields that have a diff.

    Determining if a field had a diff is as straight-forward as looking to see if it shows
    up in the parsed diff JSON.  The fields may be at the root of the diff, or they could
    be nested under "$insert" or "$delete" nodes in the diff.

    If a field from the original A/B records are not in the diff at all, then they did not
    have changes, and therefore will not receive a 1 here to indicate a diff.
    """
    fields_with_diffs = {}

    for key in diff_data:

        # identify modified fields nested in $insert or $delete blocks
        if key in ("$insert", "$delete"):
            for subfield in diff_data[key]:
                fields_with_diffs[subfield] = 1

        # identified modified fields at root of diff
        else:
            fields_with_diffs[key] = 1

    return fields_with_diffs


def calculate_metrics_data(field_matrix_parquet: str) -> dict:
    """Create a dictionary of metrics via DuckDB queries."""
    summary: dict = {}
    analysis: dict = {
        "by_source": {},
        "by_field": {},
    }

    with duckdb.connect(":memory:") as conn:

        # prepare duckdb context and init output structures
        fields, sources = _prepare_duckdb_context(conn, field_matrix_parquet)

        # get global counts across all fields and sources
        total_records, total_records_with_diff = _get_global_counts(conn)

        summary.update(
            {
                "total_records": total_records,
                "total_records_with_diff": total_records_with_diff,
                "records_with_diff_percent": round(
                    (total_records_with_diff / total_records) * 100, 2
                ),
                "sources": sources,
                "fields_with_diffs": fields,
            }
        )

        # get source oriented counts
        analysis = _get_source_counts(conn, fields, sources, analysis)

        # get field oriented counts
        analysis = _get_field_counts(conn, fields, sources, analysis)

    return {"summary": summary, "analysis": analysis}


def _prepare_duckdb_context(
    conn: DuckDBPyConnection,
    field_matrix_parquet: str,
) -> tuple[list[str], list[str]]:
    """Create views and tables that will be used throughout metrics aggregation.

    Additionally, extract some high level information like what sources and fields were
    involved in the run.  Note: if no records have a diff for a field, this field will not
    show up in the aggregate metrics at all.  This is consistent with the purpose of this
    application to show differences, where the absence of a field or source implies there
    were no changes to them.
    """
    # create view of record diff matrix
    conn.execute(
        f"""
            create view record_diff_matrix as (
            select * from '{os.path.abspath(field_matrix_parquet)}/**/*.parquet'
            );"""
    )

    # create table of field names with changes
    conn.execute(
        f"""
            create table record_fields as (
                select column_name as field_name
                from information_schema.columns
                where table_name = 'record_diff_matrix'
                and column_name not in {tuple(NON_TIMDEX_FIELD_COLUMNS)}
            );
            """
    )

    # get list of TIMDEX fields that had changes in at least 1+ records
    columns = conn.execute("PRAGMA table_info('record_diff_matrix');").fetchall()
    fields = [col[1] for col in columns if col[1] not in NON_TIMDEX_FIELD_COLUMNS]

    # get list of unique sources from the run
    sources = [
        row[0]
        for row in conn.execute(
            "select distinct source from record_diff_matrix;"
        ).fetchall()
    ]

    return fields, sources


def _get_global_counts(conn: DuckDBPyConnection) -> tuple[int, int]:
    total_records = conn.execute(
        """
        SELECT COUNT(*) FROM record_diff_matrix
        """
    ).fetchone()[0]

    total_records_with_diff = conn.execute(
        """
        select count(*) from record_diff_matrix where has_diff;
        """
    ).fetchone()[0]

    return total_records, total_records_with_diff


def _get_source_counts(
    conn: DuckDBPyConnection,
    fields: list[str],
    sources: list[str],
    analysis: dict,
) -> dict:
    for source in sources:
        total_count = conn.execute(
            f"""
            select count(*) from record_diff_matrix
            where source = '{source}'
            and has_diff;
        """
        ).fetchone()[0]

        source_counts = {"count": total_count, "field_counts": {}}

        for field in fields:
            field_count = conn.execute(
                f"""
                select count(*) from record_diff_matrix
                where source = '{source}' and {field} = 1
            """
            ).fetchone()[0]
            source_counts["field_counts"][field] = field_count

        analysis["by_source"][source] = source_counts

    return analysis


def _get_field_counts(
    conn: DuckDBPyConnection,
    fields: list[str],
    sources: list[str],
    analysis: dict,
) -> dict:
    for field in fields:
        total_field_count = conn.execute(
            f"""
            select count(*) from record_diff_matrix
            where {field} = 1
            """
        ).fetchone()[0]

        source_counts = {}
        for source in sources:
            field_count = conn.execute(
                f"""
                select count(*) from record_diff_matrix
                where source = '{source}' and {field} = 1
                """
            ).fetchone()[0]
            source_counts[source] = field_count

        analysis["by_field"][field] = {
            "count": total_field_count,
            "source_counts": source_counts,
        }

    return analysis
