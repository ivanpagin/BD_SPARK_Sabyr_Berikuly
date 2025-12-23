import time
import requests

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


# ================= CONFIG =================
INPUT_PATH = "/data/input/*.csv"
OUTPUT_PATH = "/data/output/cleaned_parquet"

OPENCAGE_KEY = "e6404c9292bf4ef98dcd78d5e0a1a484"
# =========================================


# ---------- GEOHASH (precision = 4) ----------
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

geohash_udf = udf(lambda la, lo: geohash_encode(la, lo, 4), StringType())


# ---------- OPENCAGE ----------
def build_address(row):
    parts = [row.name, row.city, row.zip, row.country]
    parts = [p for p in parts if p is not None and str(p).strip()]
    return ", ".join(map(str, parts))


def geocode_opencage(address: str):
    if not address:
        return (None, None)

    url = "https://api.opencagedata.com/geocode/v1/json"
    params = {
        "q": address,
        "key": OPENCAGE_KEY,
        "limit": 1,
        "no_annotations": 1,
    }

    try:
        r = requests.get(url, params=params, timeout=10)

        if r.status_code == 429:
            return (None, None)

        r.raise_for_status()
        data = r.json()

        if data.get("results"):
            geo = data["results"][0]["geometry"]
            return float(geo["lat"]), float(geo["lng"])
    except Exception:
        pass

    return (None, None)


def main():
    spark = SparkSession.builder.appName("ETL-Geocoding-Geohash").getOrCreate()

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("rating", DoubleType(), True),
        StructField("name", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
    ])

    # -------- EXTRACT --------
    df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(INPUT_PATH)
    )

    df = df.withColumn("lat", col("lat").cast("double")) \
           .withColumn("lon", col("lon").cast("double"))

    df_ok = df.filter(col("lat").isNotNull() & col("lon").isNotNull())
    df_bad = df.filter(col("lat").isNull() | col("lon").isNull())

    # -------- ENRICH VIA REST API --------
    fixed_rows = []

    for row in df_bad.toLocalIterator():
        address = build_address(row)
        new_lat, new_lon = geocode_opencage(address)

        final_lat = new_lat if new_lat is not None else row.lat
        final_lon = new_lon if new_lon is not None else row.lon

        fixed_rows.append((
            row.id, row.rating, row.name, row.zip,
            row.country, row.city,
            final_lat, final_lon
        ))

        time.sleep(0.3)

    df_fixed = spark.createDataFrame(fixed_rows, schema)

    # -------- MERGE + GEOHASH --------
    final_df = (
        df_ok
        .unionByName(df_fixed)
        .withColumn("etl_loaded_at", current_timestamp())
        .withColumn("geohash4", geohash_udf(col("lat"), col("lon")))
    )

    # -------- LOAD --------
    final_df.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
