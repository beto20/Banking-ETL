from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import expr, col, when

spark = SparkSession.builder \
    .master("local") \
    .appName("spark-silver") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# Functions
def persist_to_database(df, flag: str):
    match flag:
        case "person":
            table = person_sch + personal_tb
        case "company":
            table = person_sch + company_tb
        case "comper":
            table = person_sch +"."+ com_per_tb
        case "report":
            table = qualification_sch + report_tb
        case "person_dep":
            table = qualification_sch + person_dependent_tb
        case "autonomy":
            table = bank_sch + autonomy_tb
        case "credit":
            table = credit_sch + credit_tb
        case "expedient":
            table = expedient_sch + expedient_tb
        case "product_exp":
            table = expedient_sch + product_exp_tb
        case "person_exp":
            table = expedient_sch + person_exp_tb
        case _: 
            schema = ""
            table = ""

    df.write.format("jdbc").mode("overwrite") \
        .option("driver", driver) \
        .option("url", url) \
        .option("dbtable", table) \
        .option("user", user) \
        .option("password", password) \
        .save()


# Get data from bronze layer
df = spark.read.parquet("../bronze/credit/*")
df.printSchema()

# Properties
url = "jdbc:postgresql://localhost:5432/postgre"
user = "postgre"
password = "postgre"
driver = "org.postgresql.Driver"

# Schemas
person_sch = "person"
qualification_sch = "qualification"
bank_sch = "bank"
credit_sch = "credit"
expedient_sch = "expedient"

# Tables
personal_tb = "Person"
company_tb = "Company"
com_per_tb = "CompanyPerson"
report_tb = "Report"
person_dependent_tb = "PersonDependent"
autonomy_tb = "Autonomy"
credit_tb = "Credit"
expedient_tb = "Expedient"
product_exp_tb = "Product"
person_exp_tb = "Person"

# Normalized and separate into tables
## PERSON SCHEMA
df.createOrReplaceTempView("PERSONAL_DATA")
df_personal = spark.sql("SELECT id, name, lastname, gender, age, district, document, documentNumber, email, phone, occupation, annualSalary, isIndependent, isClient FROM PERSONAL_DATA")
# df_personal.printSchema()
# df_personal.show()

df_per_final = df_personal.drop(col("isIndependent"))
# df_per_final.show()
# persist_to_database(df_per_final, "person")


df.createOrReplaceTempView("COMPANY_DATA")
df_company = spark.sql("SELECT id, company, sector, isIndependent FROM COMPANY_DATA")
df_company_final = df_company.filter(col("isIndependent") == False).drop(col("isIndependent")).distinct()
# persist_to_database(df_company_final, "company")

# Crear funcion que lea el DF principal, primero filtre los dependendientes, luego contraste el nombre de compañia contra el DF company, hacer union de ambas tablas y solo quedarte con los ID

df_company_final.createOrReplaceTempView("COMPANY_DATA_CLEAN")
df_com_per_final = spark.sql(""" 
            SELECT p.id AS personId, p.name, c.id AS companyId, c.company FROM COMPANY_DATA_CLEAN AS c 
            INNER JOIN PERSONAL_DATA as p ON p.company = c.company
            WHERE p.isIndependent = False
          """)
     
# df_com_per.show(100)
persist_to_database(df_com_per_final, "comper")


## QUALIFICATION SCHEMA
df.createOrReplaceTempView("REPORT_DATA")
df_report = spark.sql("SELECT qualification, hasBlackList, creditScore FROM REPORT_DATA")
# df_report.printSchema()

df_person_dependent_final = df_personal.alias('p')\
    .join(df_company.alias('c'),col("p.id") ==  col("c.id"), "inner") \
    .select('*') \
    .drop(col("c.id")) \
    .filter(col("c.isIndependent") == False)
# df_person_dependent_final.show(truncate=False)
# persist_to_database(df_person_dependent_final, "person_dep")


## BANK SCHEMA
df.createOrReplaceTempView("AUTONONY_DATA")
df_autonomy = spark.sql("SELECT store, authorizer FROM AUTONONY_DATA")
# df_autonomy.printSchema()

## CREDITS SCHEMA
df.createOrReplaceTempView("CREDIT_DATA")
df_credit = spark.sql("SELECT creditLine, creditType, requestedAmount, agreedAmount, interestRate, term, quota, hasDebt, totalDebtAmount FROM CREDIT_DATA")
# df_credit.printSchema()

# Business logic





dt = datetime.now()
ts_str = str(datetime.timestamp(dt))
print("Timestamp:: ", ts_str)

# Move data to gold layer
# df.write.mode('overwrite').parquet('../silver/credit/' + ts_str)