import json
from difflib import unified_diff
from pathlib import Path

import duckdb
import pandas as pd
from flask import g

SPARSE_MATRIX_SKIP_FIELDS = [
    "record_a",
    "record_b",
    "ab_diff",
    "modified_timdex_fields",
    "has_diff",
]


def get_run_directory(run_timestamp: str) -> str:
    return str(Path(g.job_directory) / "runs" / run_timestamp)


def get_record_a_b_versions(
    run_directory: str, timdex_record_id: str
) -> tuple[dict, dict]:
    """Retrieve A and B versions of a single record from diffs dataset."""
    with duckdb.connect() as conn:
        parquet_glob_pattern = f"{run_directory}/records/**/*.parquet"
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
        parquet_glob_pattern = f"{run_directory}/records/**/*.parquet"
        conn.execute(
            f"""
            create view record_diff_matrix as (
                select
                * exclude ({",".join([f'"{col}"' for col in SPARSE_MATRIX_SKIP_FIELDS])})
                from read_parquet(
                    '{parquet_glob_pattern}',
                    hive_partitioning=true
                )
            );"""
        )

        # execute passed query
        conn.execute(query, parameters)
        return conn.fetchdf()


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

    timdex_fields = {
        field: value
        for field, value in record_row.items()
        if field not in SPARSE_MATRIX_SKIP_FIELDS
    }
    fields_with_diffs = [field for field, value in timdex_fields.items() if value == 1]

    return {
        "fields_with_diffs": {
            "count": len(fields_with_diffs),
            "fields": fields_with_diffs,
        }
    }


def query_duckdb_for_records_datatable(
    duckdb_filepath: str,
    draw: int = 1,
    start: int = 0,
    length: int = 10,
    search_value: str = "",
    order_column_index: int = 0,
    order_direction: str = "asc",
    source_filter: list | None = None,
    modified_fields_filter: list | None = None,
) -> dict:
    """Perform DuckDB query against Records dataset for DataTables table.

    The arguments for this function align closely with what DataTables sends to the Flask
    endpoint for data retrieval.  These are sufficient to filter and sort the data when
    querying from DuckDB.
    """
    if not source_filter:
        source_filter = []
    if not modified_fields_filter:
        modified_fields_filter = []

    # map column index to column name
    column_names = [
        "timdex_record_id",
        "source",
        "has_diff",
        "modified_timdex_fields",
    ]
    order_column = column_names[int(order_column_index)]

    with duckdb.connect(duckdb_filepath, read_only=True) as conn:
        where_clauses = []
        params = []

        # if search box used, apply full-text searches to A and B JSON records
        if search_value:
            search_value_safe = (
                "%" + search_value.replace("%", "\\%").replace("_", "\\_") + "%"
            )
            search_clause = """
                        (
                            record_a::text LIKE ?
                            OR record_b::text LIKE ?
                        )
                        """
            where_clauses.append(search_clause)
            params.extend([search_value_safe, search_value_safe])

        # filter records by source
        if source_filter:
            source_placeholders = ", ".join(["?"] * len(source_filter))
            source_clause = f"source IN ({source_placeholders})"
            where_clauses.append(source_clause)
            params.extend(source_filter)

        # filter records by rows with specific fields that have diffs
        if modified_fields_filter:
            modified_fields_placeholders = ", ".join(["?"] * len(modified_fields_filter))
            modified_fields_clause = f"""
                        EXISTS (
                            SELECT 1
                            FROM UNNEST(modified_timdex_fields) AS field(field_value)
                            WHERE field_value IN ({modified_fields_placeholders})
                        )
                    """
            where_clauses.append(modified_fields_clause)
            params.extend(modified_fields_filter)

        # build the base query
        where_query = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        base_query = f"""
                    SELECT
                        timdex_record_id,
                        source,
                        has_diff,
                        modified_timdex_fields::text AS modified_timdex_fields
                    FROM records
                    {where_query}
                """

        # get total records count (before filtering)
        records_total = conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]  # type: ignore[index]

        # get filtered records count
        records_filtered_query = f"SELECT COUNT(*) FROM records {where_query}"
        records_filtered = conn.execute(records_filtered_query, params).fetchone()[0]  # type: ignore[index]

        # apply ordering and pagination
        order_query = f"ORDER BY {order_column} {order_direction.upper()}"
        limit_query = f"LIMIT {length} OFFSET {start}"

        # prepare final query and execute query
        final_query = f"{base_query} {order_query} {limit_query}"
        records_df = conn.execute(final_query, params).fetchdf()

    data = records_df.to_dict(orient="records")

    return {
        "draw": draw,
        "recordsTotal": records_total,
        "recordsFiltered": records_filtered,
        "data": data,
    }
