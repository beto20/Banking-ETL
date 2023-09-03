from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("spark-bronze") \
    .getOrCreate()

full_path_credit_data = '../data/credit-banking-mock-data.csv'
df = spark.read.options(header = "True", inferSchema = "True").format("csv").load(full_path_credit_data)

df.write.mode('overwrite').parquet('../bronze/credit/')