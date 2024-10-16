import json
from difflib import unified_diff
from pathlib import Path

import duckdb
import pandas as pd
from flask import g

from abdiff.core.utils import read_run_json


def get_run_directory(run_timestamp: str) -> str:
    return str(Path(g.job_directory) / "runs" / run_timestamp)


def get_record_a_b_versions(
    run_directory: str, timdex_record_id: str
) -> tuple[dict, dict]:
    """Retrieve A and B versions of a single record from diffs dataset."""
    with duckdb.connect() as conn:
        parquet_glob_pattern = f"{run_directory}/diffs/**/*.parquet"
        conn.execute(
            """
            select record_a, record_b
            from read_parquet(?, hive_partitioning = true)
            where timdex_record_id = ?;
        """,
            (parquet_glob_pattern, timdex_record_id),
        )
        a, b = map(json.loads, conn.fetchone())  # type: ignore[arg-type]
    a_sorted = {key: a[key] for key in sorted(a)}
    b_sorted = {key: b[key] for key in sorted(b)}
    return a_sorted, b_sorted


def get_record_unified_diff_string(record_a: dict, record_b: dict) -> str:
    """Get a unified diff string from A and B versions.

    This is similar to what a git client will produce, and is suitable for passing to
    other libraries for rendering.
    """
    a_str = json.dumps(record_a, indent=2, sort_keys=True)
    b_str = json.dumps(record_b, indent=2, sort_keys=True)

    diff_iter = unified_diff(
        a_str.splitlines(keepends=True),
        b_str.splitlines(keepends=True),
        fromfile="Record A",
        tofile="Record B",
        n=10_000,  # ensure full records are shown
        lineterm="",
    )
    return """diff --git a/record_a.json b/record_b.json""" + "\n".join(diff_iter)


def duckdb_query_run_metrics(
    run_directory: str, query: str, parameters: tuple | list | dict | None
) -> pd.DataFrame:
    """Perform a SQL query via DuckDB of sparse record field diff matrix.

    The passed query MAY utilize the created view 'record_diff_matrix', but is not
    required to do so.
    """
    with duckdb.connect() as conn:

        # prepare view of record diff matrix
        parquet_glob_pattern = f"{run_directory}/metrics/**/*.parquet"
        conn.execute(
            f"""
            create view record_diff_matrix as (
                select * from read_parquet(
                    '{parquet_glob_pattern}',
                    hive_partitioning=true
                )
            );"""
        )

        # execute passed query
        conn.execute(query, parameters)
        return conn.fetchdf()


def get_field_sample_records(run_directory: str, field: str) -> pd.DataFrame:
    """Get sample of records where the passed field has a diff.

    This will return a maximum of 100 records from any given source, ordered by the
    timdex_record_id.
    """
    query = f"""
    select timdex_record_id, source
    from (
        select
            timdex_record_id,
            source,
            row_number() over (partition by source order by timdex_record_id) as row_num
        from record_diff_matrix
        where {field} = 1
    ) subquery
    where row_num <= 100
    ;
    """
    parameters = None
    return duckdb_query_run_metrics(run_directory, query, parameters)


def get_source_sample_records(run_directory: str, source: str) -> pd.DataFrame:
    """Get sample of records, from a given source, where a diff for any field is found.

    This will return a maximum of 100 records from any given source, ordered by the
    timdex_record_id.
    """
    run_data = read_run_json(run_directory)
    fields = run_data["metrics"]["summary"]["fields_with_diffs"]
    any_field_modified_condition = " OR ".join(f"{field} = 1" for field in fields)

    query = f"""
        select timdex_record_id, source
        from (
            select
                timdex_record_id,
                source,
                row_number() over (
                    partition by source order by timdex_record_id
                ) as row_num
            from record_diff_matrix
            where source = ?
            and ({any_field_modified_condition})
        ) subquery
        where row_num <= 100
        ;
        """
    parameters = (source,)

    return duckdb_query_run_metrics(run_directory, query, parameters)


def get_record_field_diff_summary(run_directory: str, timdex_record_id: str) -> dict:
    """Provide a summary of differences for a single record (timdex_record_id)."""
    # get fields with diffs
    results_df = duckdb_query_run_metrics(
        run_directory,
        """
        select * from record_diff_matrix
        where timdex_record_id = ?
        """,
        (timdex_record_id,),
    )
    if len(results_df) == 0:
        raise ValueError(  # noqa: TRY003
            "Could not find record in diff matrix"
            f" for timdex_record_id: {timdex_record_id}"
        )
    record_row = results_df.iloc[0].to_dict()

    skip_fields = ["timdex_record_id", "source"]
    timdex_fields = {
        field: value for field, value in record_row.items() if field not in skip_fields
    }
    fields_with_diffs = [field for field, value in timdex_fields.items() if value == 1]

    return {
        "fields_with_diffs": {
            "count": len(fields_with_diffs),
            "fields": fields_with_diffs,
        }
    }
