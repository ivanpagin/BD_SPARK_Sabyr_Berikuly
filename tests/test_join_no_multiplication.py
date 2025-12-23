from pyspark.sql import functions as F
from src.geohash import geohash_encode

def test_join_no_multiplication(spark):
    r = spark.createDataFrame(
        [("r1", "9w0p"), ("r2", "9w0p")],
        "id string, geohash4 string"
    )

    w = spark.createDataFrame(
        [
            (18.6, -111.0, 25.0, 77.0, "2017-08-28", 2017, 8, 28),
            (18.6, -111.0, 27.0, 80.6, "2017-08-29", 2017, 8, 29),
            (18.6, -111.0, 26.0, 78.8, "2017-08-27", 2017, 8, 27),
        ],
        "lat double, lng double, avg_tmpr_c double, avg_tmpr_f double, wthr_date string, year int, month int, day int"
    ).withColumn("weather_date", F.to_date("wthr_date"))

    geohash_udf = F.udf(lambda la, lo: geohash_encode(la, lo, 4), "string")
    w = w.withColumn("geohash4", geohash_udf("lat", "lng"))

    latest = w.groupBy("geohash4").agg(F.max("weather_date").alias("latest_date"))
    w_latest = w.join(
        latest,
        (w.geohash4 == latest.geohash4) & (w.weather_date == latest.latest_date),
        "inner"
    ).drop(latest.geohash4)

    w_one = w_latest.groupBy("geohash4").agg(
        F.first("avg_tmpr_c", ignorenulls=True).alias("weather_avg_tmpr_c"),
        F.first("year", ignorenulls=True).alias("year"),
        F.first("month", ignorenulls=True).alias("month"),
        F.first("day", ignorenulls=True).alias("day"),
    )

    joined = r.join(w_one, on="geohash4", how="left")
    assert joined.count() == r.count()
