# PySpark Insert via Spark Connect

This project demonstrates how to insert data into a Spark-based table (e.g., Iceberg)  
using PySpark through Spark Connect.

---

## Requirements

1. **Spark Connect Server**
    - Start Spark Connect server on your Spark cluster or EMR(master node):
     
    - Ensure that the server’s port (e.g., 15002) is accessible from your client environment.

2. **.env File**
    - In the project root, create a `.env` file:
      ```
      SPARK_CONNECT=sc://[spark driver ip]:15002
      ```
    - Example:
      ```
      SPARK_CONNECT=sc://10.0.0.10:15002
      ```

3. **Python Dependencies**
    - Install required packages (see `requirements.txt`), e.g.:
      - pyspark
      - python-dotenv
      - pydantic
      - pyarrow
      - grpcio
      - grpcio-status
      - pandas

---

## Table Creation

Before inserting any data, you **must create the target table** in Spark  
(using Spark SQL, PySpark, or Spark shell).  
Here is a typical Iceberg table creation example:

```sql
CREATE TABLE my_catalog.v1.test (
    measurement_point_id STRING NOT NULL,
    value DOUBLE,
    recorded_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (
    days(recorded_at), bucket(8, measurement_point_id)
)
COMMENT 'TEST data info'
TBLPROPERTIES ('format-version' = '2', 'write.format.default' = 'parquet');
```

---

## Contact

If you have questions, need further support, or want to collaborate,  
please reach out via:

- **Email:** sars21@hanmail.net
- **LinkedIn:** (https://www.linkedin.com/in/seokjin-shin/)

Feel free to connect!
