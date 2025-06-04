from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime
import os

load_dotenv()
spark_connect = os.getenv("SPARK_CONNECT")
spark = SparkSession.builder.remote(spark_connect).getOrCreate()

data = [
    ("31c1eb85-836d-4140-a3a3-5eb5e8035d50", 12.34, datetime.strptime("2024-06-04T12:00:00Z", "%Y-%m-%dT%H:%M:%SZ")),
    ("e788870b-7783-4bc2-81e1-56d763769c12", 23.45, datetime.strptime("2024-06-04T13:00:00Z", "%Y-%m-%dT%H:%M:%SZ")),
]
schema = StructType([
    StructField("measurement_point_id", StringType(), False),
    StructField("value", DoubleType(), True),
    StructField("recorded_at", TimestampType(), False),
])

df = spark.createDataFrame(data, schema=schema)
df.write.format("iceberg").mode("append").save("v1.test")

print("#####end")
spark.stop()
