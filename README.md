# \# Spark ETL Homework: Restaurants \& Weather Enrichment

# 

# \## Repository

# 🔗 Repository link: \*\*<https://github.com/ivanpagin/BD\_SPARK\_Sabyr\_Berikuly.git>\*\*

# 

# ---

# 

# \## Task Description

# 

# The goal of this homework was to build a Spark ETL pipeline that:

# 

# 1\. Performs a \*\*LEFT JOIN\*\* between restaurant and weather datasets using a \*\*four-character geohash\*\*.

# 2\. \*\*Avoids data multiplication\*\* when joining weather data.

# 3\. Keeps the job \*\*idempotent\*\*.

# 4\. Stores the enriched result in \*\*partitioned Parquet format\*\*.

# 5\. Uploads the full source code with \*\*tests\*\*.

# 6\. Provides full documentation with \*\*screenshots and explanations\*\*.

# 

# Development and testing were done locally using \*\*Docker + Apache Spark\*\*.

# 

# ---

# 

# \## Solution Overview

# 

# \### Key Design Decisions

# 

# \- \*\*Join key:\*\* `geohash4` (4-character geohash).

# \- \*\*Avoiding data multiplication:\*\*  

# &nbsp; Weather data may contain multiple records per geohash (different dates).  

# &nbsp; To avoid multiplying restaurant rows:

# &nbsp; - Weather data is first reduced to \*\*one row per geohash\*\*.

# &nbsp; - The \*\*latest available weather date\*\* is selected for each geohash.

# \- \*\*Idempotency:\*\*  

# &nbsp; - All transformations are deterministic.

# &nbsp; - Output is written using `mode("overwrite")`.

# \- \*\*Partitioned output:\*\*  

# &nbsp; - The final dataset is written in Parquet format partitioned by `year/month/day`.

# 

# ---

# 

# \## Project Structure

# 

# ```text

# spark-etl/

# &nbsp; src/

# &nbsp;   geohash.py

# &nbsp;   join\_weather\_restaurants\_partitioned.py

# &nbsp;   etl\_job.py	

# &nbsp; tests/

# &nbsp;   conftest.py

# &nbsp;   test\_geohash.py

# &nbsp;   test\_join\_no\_multiplication.py

# &nbsp;   test\_idempotent\_write.py

# &nbsp; Dockerfile

# &nbsp; requirements.txt

# &nbsp; README.md

# data/

# &nbsp; input/

# &nbsp;   weather/

# &nbsp;     year=2016/

# &nbsp;       month=10/

# &nbsp;         day=01/

# &nbsp;           part-\*.snappy.parquet

# &nbsp; output/

# &nbsp;   cleaned\_parquet/   (restaurants data from previous task)

# Running the ETL Job (Docker)

# 

# The ETL job is executed using Apache Spark inside Docker.

# 

# Command

# docker run --rm -it \\

# &nbsp; --memory 6g \\

# &nbsp; -v "C:\\Users\\User\\spark-etl\\data\\input:/data/input" \\

# &nbsp; -v "C:\\Users\\User\\spark-etl\\data\\output:/data/output" \\

# &nbsp; -v "C:\\Users\\User\\spark-etl:/app" \\

# &nbsp; -w /app \\

# &nbsp; apache/spark:3.5.7 \\

# &nbsp; /opt/spark/bin/spark-submit \\

# &nbsp;   --driver-memory 4g \\

# &nbsp;   --conf spark.sql.shuffle.partitions=32 \\

# &nbsp;   src/join\_weather\_restaurants\_partitioned.py

# 

# 

# Output Data

# 

# The enriched dataset is written to:

# 

# data/output/restaurants\_weather\_enriched

# 

# 

# output:

# 

# \_SUCCESS file

# 

# partition folders: year=YYYY/month=MM/day=DD

# 

# parquet files: part-\*.snappy.parquet

# 

# 

# Inspecting the Result

# Reading the output with Spark

# docker run --rm -it \\

# &nbsp; -v "C:\\Users\\User\\spark-etl\\data\\output:/data/output" \\

# &nbsp; apache/spark:3.5.7 \\

# &nbsp; /opt/spark/bin/pyspark

# 

# df = spark.read.parquet("/data/output/restaurants\_weather\_enriched")

# df.printSchema()

# df.show(10, truncate=False)

# 

# output schema

# 

# Restaurant fields

# 

# Weather fields with weather\_ prefix:

# 

# weather\_lat

# 

# weather\_lng

# 

# weather\_avg\_tmpr\_c

# 

# weather\_avg\_tmpr\_f

# 

# weather\_date

# 

# Partition columns: year, month, day

# 

# Tests

# 

# All tests are executed inside Docker using pytest.

# 

# Building test image

# docker build -t spark-etl-tests .

# 

# Running tests

# docker run --rm -it \\

# &nbsp; -v "C:\\Users\\User\\spark-etl:/app" \\

# &nbsp; -w /app \\

# &nbsp; spark-etl-tests \\

# &nbsp; python3 -m pytest -q

# 

# Test Coverage

# 

# test\_geohash.py

# Validates that geohash encoding returns a 4-character string.

# 

# test\_join\_no\_multiplication.py

# Ensures that joining weather data does not multiply restaurant rows.

# 

# test\_idempotent\_write.py

# Confirms that writing output multiple times produces consistent results.

# 

# 

# 3 passed in 17.45s

# 

# Validation Checks

# 

# LEFT JOIN preserves all restaurant records.

# 

# No row multiplication after join.

# 

# Output is idempotent.

# 

# Partitioned Parquet format is preserved.

# 

# Tests pass successfully.

# 

# Conclusion

# 

# The Spark ETL pipeline successfully enriches restaurant data with weather information using geohash-based joining.

# All task requirements are met, including correctness, idempotency, partitioning, testing, and documentation.





