import pytest

from stocklake.contracts import ALL_SCHEMAS, get_schema, to_spark


@pytest.mark.spark
def test_to_spark_all_and_empty_df():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    structs = {(s.layer, s.name): to_spark(s) for s in ALL_SCHEMAS}
    assert all(len(st.fields) for st in structs.values())
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("test_convert")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        schema = get_schema("clean", "daily_ohlcv")
        df = spark.createDataFrame([], to_spark(schema))
        assert df.columns == list(schema.column_names)
        assert df.count() == 0
    finally:
        spark.stop()
