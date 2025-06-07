from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.utils import AnalysisException
from pyspark.sql import functions
from typing import Optional, Dict
from pydantic import BaseModel
import json
import os

test_context = json.dumps({
    "type": "test",
    "building_id": "2b3c4d5e-6f7a-8b9c-0d1e-2f3a4b5c6d7e",
    "data": [
        {
            "measurement_point_id": "dcba4321-dcba-4321-dcba-87654321dcba",
            "value": 400.2,
            "recorded_at": "2024-06-07T16:35:00Z"
        },
        {
            "measurement_point_id": "dcba4321-dcba-4325-dcba-87654321dcba",
            "value": 3.1512,
            "recorded_at": "2024-06-07T16:35:00Z"
        },
        {
            "measurement_point_id": "dcba4321-dcba-4322-dcba-87654321dcba",
            "value": 123.14,
            "recorded_at": "2024-06-07T16:35:00Z"
        },
    ]
})

class SparkInsertError(Exception):
    """Custom Exception for spark_insert errors."""
    pass

class GemsData(BaseModel):
    building_id: str
    measurement_point_id: str
    wire: Optional[float]
    total_a: Optional[float]
    total_w: Optional[float]
    total_pf: Optional[float]
    r_v: Optional[float]
    r_a: Optional[float]
    r_w: Optional[float]
    r_pf: Optional[float]
    s_v: Optional[float]
    s_a: Optional[float]
    s_w: Optional[float]
    s_pf: Optional[float]
    t_v: Optional[float]
    t_a: Optional[float]
    t_w: Optional[float]
    t_pf: Optional[float]
    kwh_sum: Optional[float]
    kwh_export_sum: Optional[float]
    recorded_at: str

class IaqData(BaseModel):
    building_id: str
    measurement_point_id: str
    value: Optional[float]
    recorded_at: str

def get_schema(sensor_type):
    if sensor_type == "gems":
        return StructType([
            StructField("building_id", StringType(), False),
            StructField("measurement_point_id", StringType(), False),
            StructField("wire", DoubleType(), True),
            StructField("total_a", DoubleType(), True),
            StructField("total_w", DoubleType(), True),
            StructField("total_pf", DoubleType(), True),
            StructField("r_v", DoubleType(), True),
            StructField("r_a", DoubleType(), True),
            StructField("r_w", DoubleType(), True),
            StructField("r_pf", DoubleType(), True),
            StructField("s_v", DoubleType(), True),
            StructField("s_a", DoubleType(), True),
            StructField("s_w", DoubleType(), True),
            StructField("s_pf", DoubleType(), True),
            StructField("t_v", DoubleType(), True),
            StructField("t_a", DoubleType(), True),
            StructField("t_w", DoubleType(), True),
            StructField("t_pf", DoubleType(), True),
            StructField("kwh_sum", DoubleType(), True),
            StructField("kwh_export_sum", DoubleType(), True),
            StructField("recorded_at", StringType(), False),
        ])

    if sensor_type == "iaq":
        return StructType([
            StructField("building_id", StringType(), False),
            StructField("measurement_point_id", StringType(), False),
            StructField("value", DoubleType(), True),
            StructField("recorded_at", StringType(), False),
        ])

    if sensor_type == "test":
        return StructType([
            StructField("measurement_point_id", StringType(), False),
            StructField("value", DoubleType(), True),
            StructField("recorded_at", StringType(), False),
        ])

    raise ValueError(f"Unknown sensor type: {sensor_type}")

def spark_insert(context):
    spark: Optional[SparkSession] = None
    building_id: Optional[str] = None
    sensor_type: Optional[str] = None

    try:
        if isinstance(context, str):
            body: Dict = json.loads(context)
        elif isinstance(context, dict):
            body = context
        else:
            raise SparkInsertError("context must be str or dict")

        sensor_type = body.get("type")
        building_id = body.get("building_id")
        data = body.get("data")

        if not (sensor_type and building_id and data):
            raise SparkInsertError(f"Missing field: type/building_id/data [building_id:{building_id}] [type:{sensor_type}]")

        schema: StructType = get_schema(sensor_type)

        load_dotenv()
        spark_connect: Optional[str] = os.getenv("SPARK_CONNECT")
        if not spark_connect:
            raise SparkInsertError(f"SPARK_CONNECT ENV not set [building_id:{building_id}] [type:{sensor_type}]")

        spark = SparkSession.builder.remote(spark_connect).getOrCreate()

        df: DataFrame = spark.createDataFrame(data, schema=schema)
        df = df.withColumn("recorded_at", functions.to_timestamp("recorded_at"))

        df.write.format("iceberg").mode("append").save("v1.test")
        print(f"##### Spark Insert End [building_id:{building_id}] [type:{sensor_type}] #####")
        return True

    except json.JSONDecodeError as e:
        print(f"[INPUT ERROR][building_id:{building_id}][type:{sensor_type}] wrong JSON: {e}")
        return False

    except AnalysisException as e:
        print(f"[SPARK ANALYSIS ERROR][building_id:{building_id}][type:{sensor_type}] {e}")
        return False

    except SparkInsertError as e:
        print(f"[USAGE ERROR][building_id:{building_id}][type:{sensor_type}] {e}")
        return False

    except Exception as e:
        print(f"[UNEXPECTED ERROR][building_id:{building_id}][type:{sensor_type}] {e.__class__.__name__}: {e}")
        return False

    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception as e:
                print(f"[SPARK STOP ERROR][building_id:{building_id}][type:{sensor_type}] {e}")