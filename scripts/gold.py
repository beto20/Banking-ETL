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
# df.printSchema()
# df.show()

# Business logic

# 1)Segun la respuesta de calificacion
def get_qualification(df):
    df_approved = df.filter(df.qualification == 'APROBADO')
    df_investigate = df.filter(df.qualification == 'INVESTIGADO')
    df_rejected = df.filter(df.qualification == 'RECHAZADO')
    df_without_qualification = df.filter((df.qualification != 'APROBADO') & (df.qualification != 'INVESTIGADO') & (df.qualification != 'RECHAZADO'))
    # df_approved.show()
    # df_investigate.show()
    # df_rejected.show()
    # df_without_qualification.show()
    # persist to aws redshift

get_qualification(df)

# 2)Segun monto mayor a amountK
def get_credit_greater_than_requested_amount(amount, df):
    df_credit = df.filter(df.agreedAmount >= amount)
    # df_credit.show()
    
get_credit_greater_than_requested_amount(250000, df)
    
# 3)
# Edad
# Adulto joven: 18 < x < 30
# Adulto mayor: 31< x < 60
# Adulto retirado: 61< x < 100
def filter_by_age(df):
    df_age1 = df.filter((df.age > 18) & (df.age < 30))
    df_age2 = df.filter((df.age > 31) & (df.age < 60))
    df_age3 = df.filter((df.age > 60) & (df.age < 100))
    # df_age1.show()
    # df_age2.show()
    # df_age3.show()

filter_by_age(df)
    
# 4)
# sum(creditos entregados) / # de solicitantes
def get_credit_average(df):
    df_average = df.filter(df.qualification == 'APROBADO').agg({"agreedAmount": "avg"})
    # df_average.show()
    
get_credit_average(df)
    
# 6) Segun respuesta de columna
def get_blacklist(df):
    df_true = df.filter(df.hasBlacklist)
    df_false = df.filter(~(df.hasBlacklist))
    # df_true.show()
    # df_false.show()
    
get_blacklist(df)

# 7, 8) En base a la calificacion, blacklist, credito otorgado, salario, deudas y linea de credito
# Potencial
def get_potential_client(df):
    df_potential = df.filter((df.qualification == 'APROBADO') \
        & ~(df.hasBlacklist) \
        & ~(df.hasDebt) \
        & (df.age > 25) & (df.age < 45) \
        & (df.annualSalary >= (df.agreedAmount * 0.35)) \
        & (df.creditScore > 0.60))
    
    # df_potential.show()

get_potential_client(df)

# Riesgo
def get_risk_client(df):
    df_risk = df.filter(((df.qualification == 'RECHAZADO') | (df.qualification == 'INVESTIGADO')) \
    & (df.hasBlacklist) \
    & (df.hasDebt) \
    & (df.age > 18) \
    & (df.annualSalary >= (df.agreedAmount * 0.55)) \
    & (df.creditScore < 0.35))

    # df_risk.show()

get_risk_client(df)


dt = datetime.now()
ts_str = str(datetime.timestamp(dt))
print("Timestamp:: ", ts_str)

# df.write.mode('overwrite').parquet('../gold/credit/' + ts_str)