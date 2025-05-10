import pandas as pd
import pytest

@pytest.mark.parametrize("table_name", [
    "admissions", "lab_results", "lab_tests", "patient_information"
])
def test_data_alignment(pg_engine, download_parquet, table_name):
    # load Postgres table into a DataFrame
    df_pg = pd.read_sql_table(table_name, pg_engine, schema="public").sort_index(axis=1)
    # load Parquet DataFrame
    df_pa = download_parquet(table_name).sort_index(axis=1)

    # 1) Row count
    assert len(df_pg) == len(df_pa), f"Row count mismatch in {table_name}"

    # 2) Full comparison
    # reset index to ensure identical ordering
    df_pg = df_pg.reset_index(drop=True)
    df_pa = df_pa.reset_index(drop=True)

    # compare all cells
    pd.testing.assert_frame_equal(
        df_pg, df_pa,
        check_dtype=False,  # allow minor type differences
        check_exact=False,  # allow floating‐point tolerance
        atol=1e-6
    )
