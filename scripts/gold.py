from pyspark.sql import SparkSession
from datetime import datetime
import os

spark = SparkSession.builder \
    .master("local") \
    .appName("spark-gold") \
    .getOrCreate()

def get_lastest_timestamp(dir_path):
    res = []
    try:
        for file_name in os.listdir(dir_path):
            file_name_float = float(file_name)
            res.append(file_name_float)
    except FileNotFoundError:
        print("Directory not found: {dir_path}")
    except PermissionError:
        print("Permission denied to access directory: {dir_path}")
    return max(res)

dir_path = '../silver/credit/'
file_name = str(get_lastest_timestamp(dir_path))
print("filename:: ", file_name)

df = spark.read.parquet("../silver/credit/" + file_name)
df.printSchema()
# df.show()

# Business logic








dt = datetime.now()
ts_str = str(datetime.timestamp(dt))
print("Timestamp:: ", ts_str)

# df.write.mode('overwrite').parquet('../gold/credit/' + ts_str)