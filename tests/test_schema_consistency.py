import pytest

def pg_schema(pg_engine, table_name):
    sql = f"""
      SELECT column_name, data_type, is_nullable, ordinal_position
      FROM information_schema.columns
      WHERE table_schema='public'
        AND table_name='{table_name}'
      ORDER BY ordinal_position;
    """
    return pg_engine.execute(sql).fetchall()

def pa_schema(df):
    # returns list of (name, pa_type, nullable)
    return [
        (field.name, str(field.type), field.nullable)
        for field in df.schema
    ]

@pytest.mark.parametrize("table_name", [
    "admissions", "lab_results", "lab_tests", "patient_information"
])
def test_schema_consistency(pg_engine, download_parquet, table_name):
    # 1) Postgres
    pg_cols = pg_schema(pg_engine, table_name)

    # 2) Parquet
    df = download_parquet(table_name)
    pa_cols = pa_schema(df)

    # 3) Compare names & count
    assert len(pg_cols) == len(pa_cols), "Column count mismatch"
    for (pg_name, pg_type, pg_nullable, _), (pa_name, pa_type, pa_nullable) in zip(pg_cols, pa_cols):
        assert pg_name == pa_name, f"Name mismatch: {pg_name} vs {pa_name}"
        # simple type mapping check
        assert pg_type.lower() in pa_type.lower(), f"Type mismatch on {pg_name}"
        assert (pg_nullable == "YES") == pa_nullable, f"Nullability mismatch on {pg_name}"
