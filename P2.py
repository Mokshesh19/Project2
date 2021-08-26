import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Project2').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")     # Default is INFO.


df = spark.read.option("header",True) \
    .csv("P2/registered_companies.csv")
df.printSchema()

df = df.drop("LATEST_YEAR_ANNUAL_RETURN", "LATEST_YEAR_FINANCIAL_STATEMENT")

df = df.withColumn('DATE_OF_REGISTRATION',to_date("DATE_OF_REGISTRATION", 'dd-MM-yyyy'))\
        .withColumn("AUTHORIZED_CAP",col("AUTHORIZED_CAP").cast("int"))\
        .withColumn("PAIDUP_CAPITAL",col("PAIDUP_CAPITAL").cast("int"))
df.printSchema()

df.createOrReplaceTempView("companies")

#---------------------------------------------------------------------------------------------------------------------------------------------
#2
print("Q.2 Total number of companies of each status..")
df3=spark.sql("Select COMPANY_STATUS, count(CORPORATE_IDENTIFICATION_NUMBER)\
as Number_of_Companies from companies group by COMPANY_STATUS")
#df3.show()
df3.select(when(df3.COMPANY_STATUS=="ACTV","Active") \
        .when(df3.COMPANY_STATUS=="NAEF","Unavailable") \
        .when(df3.COMPANY_STATUS=="ULQD","under liquidtaion")\
        .when(df3.COMPANY_STATUS=="AMAL","Amalgated")\
        .when(df3.COMPANY_STATUS=="STOF","Strike off")\
        .when(df3.COMPANY_STATUS=="DISD","Dissolved")\
        .when(df3.COMPANY_STATUS=="CLLD","Conv to LLP")\
        .when(df3.COMPANY_STATUS=="UPSO","Under process of SO")\
        .when(df3.COMPANY_STATUS=="CLLP","Converted LLP")\
        .when(df3.COMPANY_STATUS=="LIQD","Liquidated")\
        .when(df3.COMPANY_STATUS=="DRMT","Dormant")\
        .when(df3.COMPANY_STATUS=="MLIQ","Vanished")\
        .when(df3.COMPANY_STATUS=="D455","Dormant under 455")
        .when(col("COMPANY_STATUS").isNull() ,"undefined") \
        .otherwise(df3.COMPANY_STATUS).alias("STATUS"),"Number_of_Companies"\
).show()
#---------------------------------------------------------------------------------------------------------------------------------------------
#4
print("RESULT OF QUERY NO.4 ----> ")
df.groupBy("PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN") \
    .agg(count("CORPORATE_IDENTIFICATION_NUMBER").alias("Number_of_Companies")) \
    .show(truncate=False)


spark.sql("Select PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN , count(CORPORATE_IDENTIFICATION_NUMBER) \
as Number_of_Companies from companies group by PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN").show(truncate =False)

#---------------------------------------------------------------------------------------------------------------------------------------------
#6
print("6.Details of duplicate company names..")
spark.sql("Select COMPANY_NAME,num from(select COMPANY_NAME,count(COMPANY_NAME) as num from companies group by COMPANY_NAME) as statistic where num >1 sort by num desc").show(truncate=False)
#---------------------------------------------------------------------------------------------------------------------------------------------
#8
print("RESULT OF QUERY NO.8 ----> ")
spark.sql("Select COMPANY_NAME, DATE_OF_REGISTRATION from companies where \
    DATE_OF_REGISTRATION >= '1990-01-01' and  DATE_OF_REGISTRATION <= '2020-12-31' Order by DATE_OF_REGISTRATION ").show()

#---------------------------------------------------------------------------------------------------------------------------------------------
#10
print("10.List all the public companies which are under liquidation or liquidated in India and belong to State-govt or Union Govt registered after 1985")
spark.sql("Select COMPANY_NAME,COMPANY_CLASS from companies where COMPANY_STATUS in ('ULQD','LIQD') and COMPANY_CLASS like 'Public' and DATE_OF_REGISTRATION >= '1985-12-31'").show(truncate =False)
#---------------------------------------------------------------------------------------------------------------------------------------------
#12
print("RESULT OF QUERY NO.12 ----> ")
spark.sql("Select COMPANY_NAME,COMPANY_CLASS,COMPANY_STATUS, \
    DATE_OF_REGISTRATION,REGISTERED_STATE,REGISTRAR_OF_COMPANIES  from companies where COMPANY_STATUS = 'MLIQ' ").show()
#---------------------------------------------------------------------------------------------------------------------------------------------
#14
print("14.List the private(one person company) companies and email address of its owner registered in Mumbai or Delhi ROC.")
spark.sql("Select COMPANY_NAME,COMPANY_CLASS, EMAIL_ADDR from companies where COMPANY_CLASS like 'Private(One Person Company)' and REGISTERED_STATE in ('Maharastra','Delhi')").show(truncate =False)
#---------------------------------------------------------------------------------------------------------------------------------------------
#16
print("RESULT OF QUERY NO.16 ----> ")
spark.sql("Select COMPANY_NAME, COMPANY_CLASS,AUTHORIZED_CAP, PAIDUP_CAPITAL from \
    companies where COMPANY_CLASS = 'Private(One Person Company)' and PAIDUP_CAPITAL > 10000000 limit 100 ").show(truncate = False) 

#---------------------------------------------------------------------------------------------------------------------------------------------
#18
print("18.List the oil companies which are private .")
spark.sql("Select COMPANY_NAME,COMPANY_CLASS from companies where COMPANY_NAME like '%OIL%' and COMPANY_CLASS like 'Private%'").show(truncate =False)
#---------------------------------------------------------------------------------------------------------------------------------------------
#20
print("RESULT OF QUERY NO.20 ----> ")
spark.sql("Select COMPANY_NAME,COMPANY_CLASS, COMPANY_SUB_CATEGORY,COMPANY_STATUS, DATE_OF_REGISTRATION,REGISTERED_STATE,\REGISTRAR_OF_COMPANIES \
    from companies where REGISTERED_STATE in ('Tamil Nadu', 'West Bengal', 'Delhi') \
        and COMPANY_SUB_CATEGORY = 'Subsidiary of Foreign Company' ").show()




# root
#  |-- CORPORATE_IDENTIFICATION_NUMBER: string (nullable = true)
#  |-- COMPANY_NAME: string (nullable = true)
#  |-- COMPANY_STATUS: string (nullable = true)
#  |-- COMPANY_CLASS: string (nullable = true)
#  |-- COMPANY_CATEGORY: string (nullable = true)
#  |-- COMPANY_SUB_CATEGORY: string (nullable = true)
#  |-- DATE_OF_REGISTRATION: string (nullable = true)
#  |-- REGISTERED_STATE: string (nullable = true)
#  |-- AUTHORIZED_CAP: string (nullable = true)
#  |-- PAIDUP_CAPITAL: string (nullable = true)
#  |-- INDUSTRIAL_CLASS: string (nullable = true)
#  |-- PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN: string (nullable = true)
#  |-- REGISTERED_OFFICE_ADDRESS: string (nullable = true)
#  |-- REGISTRAR_OF_COMPANIES: string (nullable = true)
#  |-- EMAIL_ADDR: string (nullable = true)
#  |-- LATEST_YEAR_ANNUAL_RETURN: string (nullable = true)
#  |-- LATEST_YEAR_FINANCIAL_STATEMENT: string (nullable = true)