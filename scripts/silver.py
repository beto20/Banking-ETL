from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.master("local[1]").appName("spark-silver") \
    .config("spark.jars", "../util/postgresql-42.6.0.jar") \
    .getOrCreate()

# Get data from bronze layer
df = spark.read.parquet("../bronze/credit/*")
df.printSchema()

# Properties
url = "jdbc:postgresql://localhost:5432/postgre"
user = "postgre"
password = "postgre"
driver = "org.postgresql.Driver"

# Tables
personal_tb = "person"
contactability_tb = "contactability"
occupation_tb = "occupation"
restriction_tb = "restriction"
credit_tb = "credit"
debts_tb = "debts"
company_tb = "company"
store_tb = "store"

# Normalized and separate into tables
df.createOrReplaceTempView("PERSONAL_DATA")
df_personal = spark.sql("SELECT id, name, lastname, gender, age, district, document, documentNumber, isClient FROM PERSONAL_DATA")
df_personal.printSchema()

df.createOrReplaceTempView("CONTACTABILITY_DATA")
df_contactability = spark.sql("SELECT email, phone FROM CONTACTABILITY_DATA")
df_contactability.printSchema()

df.createOrReplaceTempView("OCCUPATION_DATA")
df_occupation = spark.sql("SELECT occupation, annualSalary, isIndependent FROM OCCUPATION_DATA")
df_occupation.printSchema()

df.createOrReplaceTempView("PERSON_RESTRICTION_DATA")
df_person_restriction = spark.sql("SELECT qualification, hasBlackList FROM PERSON_RESTRICTION_DATA")
df_person_restriction.printSchema()

df.createOrReplaceTempView("CREDIT_DATA")
df_credit = spark.sql("SELECT creditScore, creditLine, creditType, requestedAmount, agreedAmount, interestRate FROM CREDIT_DATA")
df_credit.printSchema()

df.createOrReplaceTempView("PENDING_DEBT_DATA")
df_pending_debt = spark.sql("SELECT term, quota, hasDebt, totalDebtAmount FROM PENDING_DEBT_DATA")
df_pending_debt.printSchema()

df.createOrReplaceTempView("COMPANY_DATA")
df_company = spark.sql("SELECT company, sector FROM COMPANY_DATA")
df_company.printSchema()

df.createOrReplaceTempView("STORE_DATA")
df_store = spark.sql("SELECT store, authorizer FROM STORE_DATA")
df_store.printSchema()

# Business logic



# df3 = spark.read.format("jdbc").option("driver", driver).option("url", url).option("dbtable", table).option("user", user).option("password", password).load()



# Persist to PG database
# df2.write.format("jdbc").mode("overwrite").option("driver", driver).option("url", url).option("dbtable", table).option("user", user).option("password", password).save()




dt = datetime.now()
ts_str = str(datetime.timestamp(dt))
print("Timestamp:: ", ts_str)

# Move data to gold layer
# df.write.mode('overwrite').parquet('../silver/credit/' + ts_str)