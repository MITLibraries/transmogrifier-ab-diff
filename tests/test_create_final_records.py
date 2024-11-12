from abdiff.core.utils import load_dataset


def test_create_final_records_dataset_success(
    diffs_dataset_directory, diff_matrix_dataset_filepath, final_records_dataset_path
):
    diffs_df = load_dataset(diffs_dataset_directory).to_table().to_pandas()
    metrics_df = load_dataset(diff_matrix_dataset_filepath).to_table().to_pandas()
    records_df = load_dataset(final_records_dataset_path).to_table().to_pandas()

    assert (
        set(diffs_df.timdex_record_id)
        == set(metrics_df.timdex_record_id)
        == set(records_df.timdex_record_id)
    )


def test_create_final_records_dataset_contains_all_expected_columns(
    diffs_dataset_directory, diff_matrix_dataset_filepath, final_records_dataset_path
):
    diffs_ds = load_dataset(diffs_dataset_directory)
    metrics_ds = load_dataset(diff_matrix_dataset_filepath)
    records_ds = load_dataset(final_records_dataset_path)

    # contain all columns from diffs dataset
    assert set(diffs_ds.schema.names).issubset(set(records_ds.schema.names))

    # contain all columns from metrics dataset
    # NOTE: this includes n number of TIMDEX fields that can *dynamically* show up in this
    #  dataset
    assert set(metrics_ds.schema.names).issubset(set(records_ds.schema.names))
