from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime

# Spark Session

spark = SparkSession.builder \
    .appName("IcebergInsert") \
    .getOrCreate()


data_set = [
    {
        "measurement_point_id": "31c1eb85-836d-4140-a3a3-5eb5e8035d50",
        "value": 12.34,
        "recorded_at": "2024-06-04T12:00:00Z"
    },
    {
        "measurement_point_id": "e788870b-7783-4bc2-81e1-56d763769c12",
        "value": 23.45,
        "recorded_at": "2024-06-04T13:00:00Z"
    }
]

schema = StructType([
    StructField("measurement_point_id", StringType(), False),
    StructField("value", DoubleType(), True),
    StructField("recorded_at", TimestampType(), False),
])

# 3. String → datetime 변환
def parse_row(row):
    return (
        row["measurement_point_id"],
        float(row["value"]) if row["value"] is not None else None,
        datetime.strptime(row["recorded_at"], "%Y-%m-%dT%H:%M:%SZ"),
    )

converted_data = [parse_row(r) for r in data_set]

sdf = spark.createDataFrame(converted_data, schema=schema)

sdf.write.format("iceberg") \
    .mode("append") \
    .save("s3catalog.db.table")

spark.stop()
