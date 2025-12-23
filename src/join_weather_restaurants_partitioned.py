from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ========= DOCKER PATHS =========
RESTAURANTS_PATH = "/data/output/cleaned_parquet"
WEATHER_PATH     = "/data/input/weather"
OUTPUT_PATH      = "/data/output/restaurants_weather_enriched"
# ================================


# ---------- GEOHASH (precision=4) ----------
_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"

def geohash_encode(lat, lon, precision=4):
    if lat is None or lon is None:
        return None

    lat_interval = [-90.0, 90.0]
    lon_interval = [-180.0, 180.0]
    geohash = []
    bit = 0
    ch = 0
    even = True

    while len(geohash) < precision:
        if even:
            mid = (lon_interval[0] + lon_interval[1]) / 2
            if lon >= mid:
                ch |= 1 << (4 - bit)
                lon_interval[0] = mid
            else:
                lon_interval[1] = mid
        else:
            mid = (lat_interval[0] + lat_interval[1]) / 2
            if lat >= mid:
                ch |= 1 << (4 - bit)
                lat_interval[0] = mid
            else:
                lat_interval[1] = mid

        even = not even
        if bit < 4:
            bit += 1
        else:
            geohash.append(_BASE32[ch])
            bit = 0
            ch = 0

    return "".join(geohash)


def main():
    spark = (
        SparkSession.builder
        .appName("Restaurants-Weather-Enrichment-Geohash4")
        .getOrCreate()
    )

    # ========= READ RESTAURANTS =========
    restaurants = spark.read.parquet(RESTAURANTS_PATH)

    if "geohash4" not in restaurants.columns:
        raise ValueError("Restaurants parquet must contain column 'geohash4'")

    # ========= READ WEATHER =========
    weather = spark.read.parquet(WEATHER_PATH)

    # expected columns (confirmed by you):
    # lat, lng, avg_tmpr_c, avg_tmpr_f, wthr_date, year, month, day

    weather = (
        weather
        .withColumn("lat", F.col("lat").cast("double"))
        .withColumn("lng", F.col("lng").cast("double"))
        .withColumn("weather_date", F.to_date(F.col("wthr_date")))
    )

    # ========= ADD GEOHASH TO WEATHER =========
    geohash_udf = F.udf(lambda la, lo: geohash_encode(la, lo, 4), "string")

    weather = weather.withColumn(
        "geohash4",
        geohash_udf(F.col("lat"), F.col("lng"))
    )

    # ========= AVOID DATA MULTIPLICATION =========
    # 1) pick latest weather_date per geohash4
    latest_date = (
        weather
        .groupBy("geohash4")
        .agg(F.max("weather_date").alias("latest_date"))
    )

    weather_latest = (
        weather
        .join(
            latest_date,
            (weather.geohash4 == latest_date.geohash4) &
            (weather.weather_date == latest_date.latest_date),
            "inner"
        )
        .drop(latest_date.geohash4)
    )

    # 2) collapse to exactly 1 row per geohash4 (deterministic)
    weather_one = (
        weather_latest
        .groupBy("geohash4")
        .agg(
            F.first("lat", ignorenulls=True).alias("weather_lat"),
            F.first("lng", ignorenulls=True).alias("weather_lng"),
            F.first("avg_tmpr_c", ignorenulls=True).alias("weather_avg_tmpr_c"),
            F.first("avg_tmpr_f", ignorenulls=True).alias("weather_avg_tmpr_f"),
            F.first("weather_date", ignorenulls=True).alias("weather_date"),
            F.first("year", ignorenulls=True).alias("year"),
            F.first("month", ignorenulls=True).alias("month"),
            F.first("day", ignorenulls=True).alias("day"),
        )
    )

    # ========= LEFT JOIN =========
    enriched = restaurants.join(weather_one, on="geohash4", how="left")

    # ========= WRITE PARQUET (IDEMPOTENT + PARTITIONED) =========
    (
        enriched.write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(OUTPUT_PATH)
    )

    spark.stop()


if __name__ == "__main__":
    main()
