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
#1

query="select REGISTERED_STATE, count(COMPANY_NAME) as count from companies group by REGISTERED_STATE"
spark.sql(query).show(30)

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
print("#4 Number of companies in each of principal business activity ----> ")

df.groupBy("PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN") \
    .agg(count("CORPORATE_IDENTIFICATION_NUMBER").alias("Number_of_Companies")) \
    .show(truncate=False)

#---------------------------------------------------------------------------------------------------------------------------------------------
#5

df.groupBy(year("DATE_OF_REGISTRATION").alias("Year"))\
   .agg(count("CORPORATE_IDENTIFICATION_NUMBER").alias("no_of_companies"))\
   .orderBy("Year").show(1000)

#---------------------------------------------------------------------------------------------------------------------------------------------
#6
print("6.Details of duplicate company names..")
spark.sql("Select COMPANY_NAME,num from(select COMPANY_NAME,count(COMPANY_NAME) as num from companies group by COMPANY_NAME) as statistic where num >1 sort by num desc").show(truncate=False)

#---------------------------------------------------------------------------------------------------------------------------------------------
#8
print("#8 List of companies registered between 1990-2020 in Arunchal Pradesh,Lakshadweep,Mizoram,Nagaland ----> ")

df.select("COMPANY_NAME", "COMPANY_STATUS", "DATE_OF_REGISTRATION","REGISTERED_STATE" )\
    .filter((df.DATE_OF_REGISTRATION >= '1990-01-01') & (df.DATE_OF_REGISTRATION <= '2020-12-31')\
         & (df.REGISTERED_STATE.isin('Lakshadweep','Mizoram','Arunachal Pradesh', 'Nagaland')))\
    .show(1500)

#---------------------------------------------------------------------------------------------------------------------------------------------
#9

q9="select COMPANY_NAME from companies where COMPANY_CLASS='Private' and COMPANY_STATUS='ACTV' and (REGISTERED_STATE= 'Delhi' or REGISTERED_STATE='Karnataka') and AUTHORIZED_CAP > 30000000"
spark.sql(q9).show()

#---------------------------------------------------------------------------------------------------------------------------------------------
#10
print("10.List all the public companies which are under liquidation or liquidated in India and belong to State-govt or Union Govt registered after 1985")
spark.sql("Select COMPANY_NAME,COMPANY_CLASS from companies where COMPANY_STATUS in ('ULQD','LIQD') and COMPANY_CLASS like 'Public' and DATE_OF_REGISTRATION >= '1985-12-31'").show(truncate =False)
#---------------------------------------------------------------------------------------------------------------------------------------------
#12
print("#12 List of companies which are vanished ----->")

df.select('COMPANY_NAME','COMPANY_CLASS','COMPANY_STATUS', 'DATE_OF_REGISTRATION','REGISTERED_STATE','REGISTRAR_OF_COMPANIES')\
    .filter(df.COMPANY_STATUS == 'MLIQ').show()

#---------------------------------------------------------------------------------------------------------------------------------------------
#13

q13="select COMPANY_NAME , DATE_OF_REGISTRATION from companies order By DATE_OF_REGISTRATION desc "
spark.sql(q13).show(50)

#-----------------------------------------------------------------------------------------------------------------------------------------------
#14
print("14.List the private(one person company) companies and email address of its owner registered in Mumbai or Delhi ROC.")
spark.sql("Select COMPANY_NAME,COMPANY_CLASS, EMAIL_ADDR from companies where COMPANY_CLASS like 'Private(One Person Company)' and REGISTERED_STATE in ('Maharastra','Delhi')").show(truncate =False)
#---------------------------------------------------------------------------------------------------------------------------------------------
#16
print("#16 List of Private(One Person Company) whose paidup capital is greater than 1 CR ----> ")

df.select('COMPANY_NAME', 'COMPANY_CLASS','AUTHORIZED_CAP', 'PAIDUP_CAPITAL')\
    .filter((df.COMPANY_CLASS == 'Private(One Person Company)') & (df.PAIDUP_CAPITAL > 10000000)).show()

#---------------------------------------------------------------------------------------------------------------------------------------------
#17

q17="select * from companies where COMPANY_SUB_CATEGORY='State Govt company' and (REGISTERED_STATE='Gujarat' or REGISTERED_STATE='Karnataka')"
spark.sql(q17).show()

#----------------------------------------------------------------------------------------------------------------------------------------------
#18
print("18.List the oil companies which are private .")
spark.sql("Select COMPANY_NAME,COMPANY_CLASS from companies where COMPANY_NAME like '%OIL%' and COMPANY_CLASS like 'Private%'").show(truncate =False)
#---------------------------------------------------------------------------------------------------------------------------------------------
#20
print("#20 List of companies which are subsidiaries of foreign company and belong to Tamil Nadu, West Bengal, Delhi ")

df.select('COMPANY_NAME','COMPANY_CLASS', 'COMPANY_SUB_CATEGORY','COMPANY_STATUS', 'DATE_OF_REGISTRATION','REGISTERED_STATE','REGISTRAR_OF_COMPANIES')\
    .filter((df.REGISTERED_STATE.isin('Tamil Nadu', 'West Bengal', 'Delhi')) & (df.COMPANY_SUB_CATEGORY == 'Subsidiary of Foreign Company')).show()





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
