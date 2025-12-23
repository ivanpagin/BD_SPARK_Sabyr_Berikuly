def test_idempotent_write(spark, tmp_path):
    df = spark.createDataFrame(
        [("a", 1, 2020, 1, 1), ("b", 2, 2020, 1, 1)],
        "id string, v int, year int, month int, day int"
    )

    out = str(tmp_path / "out_parquet")

    df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(out)
    c1 = spark.read.parquet(out).count()

    df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(out)
    c2 = spark.read.parquet(out).count()

    assert c1 == c2 == 2
