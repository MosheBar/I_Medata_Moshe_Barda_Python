import pandas as pd
import pytest

TABLE = "lab_tests"  # choose one table for simplicity
PK = "test_id"

@pytest.fixture
def cleanup(pg_engine):
    yield
    # rollback any test rows after each test
    pg_engine.execute(f"DELETE FROM {TABLE} WHERE {PK} LIKE 'ZZZ_%'")

def test_crud_reflection(pg_engine, download_parquet, cleanup):
    # 1) CREATE
    pg_engine.execute(f"""
      INSERT INTO {TABLE} ({PK}, patient_id, test_name, order_date, order_time, ordering_physician)
      VALUES ('ZZZ_1','999999','CRUD Test','2025-01-01','00:00:00','Dr. Test')
    """)
    df_pa = download_parquet(TABLE)
    assert 'ZZZ_1' in df_pa[PK].values

    # 2) READ (implicit above)

    # 3) UPDATE
    pg_engine.execute(f"""
      UPDATE {TABLE} SET test_name='CRUD Updated' WHERE {PK}='ZZZ_1'
    """)
    df_pa = download_parquet(TABLE)
    assert df_pa.loc[df_pa[PK]=='ZZZ_1','test_name'].iloc[0] == 'CRUD Updated'

    # 4) DELETE
    pg_engine.execute(f"DELETE FROM {TABLE} WHERE {PK}='ZZZ_1'")
    df_pa = download_parquet(TABLE)
    assert 'ZZZ_1' not in df_pa[PK].values
