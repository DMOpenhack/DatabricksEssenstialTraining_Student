# Databricks notebook source
# MAGIC %md
# MAGIC # Data Engineering with Lakehouse Platform
# MAGIC 
# MAGIC Delta Lake: An open-source storage format that brings ACID transactions to Apache Spark™ and big data workloads. This is a quick 101 introduction some of Delta Lake features.
# MAGIC 
# MAGIC 
# MAGIC <img src="https://github.com/davidma3768/images/blob/master/databricks/Data%20Engineering%20with%20Lakehouse%20Platform.jpg?raw=true/">

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Ingestion Architecture - Delta Lake
# MAGIC <img src="https://github.com/davidma3768/images/blob/master/databricks/Delta_Ingestion_methods.png?raw=true" width=1200/>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a sample database, a sample table, load some data 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE Database if not exists  db1 ;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS db1.loan_risks_upload;
# MAGIC 
# MAGIC CREATE TABLE db1.loan_risks_upload (
# MAGIC   loan_id BIGINT,
# MAGIC   funded_amnt INT,
# MAGIC   paid_amnt DOUBLE,
# MAGIC   addr_state STRING
# MAGIC ) USING DELTA LOCATION '/mnt/deltalake/name/loan_risks_upload';

# COMMAND ----------

# DBTITLE 1,Load data from source into the Delta Lake table
# MAGIC %sql
# MAGIC COPY INTO db1.loan_risks_upload
# MAGIC FROM '/databricks-datasets/learning-spark-v2/loans/loan-risks.snappy.parquet'
# MAGIC FILEFORMAT = PARQUET;

# COMMAND ----------

# DBTITLE 1,Query the Delta table
# MAGIC %sql
# MAGIC select count(*) from db1.loan_risks_upload;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db1.loan_risks_upload;

# COMMAND ----------

# DBTITLE 1,INSERT
# MAGIC %sql
# MAGIC Insert into db1.loan_risks_upload Select * from db1.loan_risks_upload

# COMMAND ----------

# DBTITLE 1,Select with Join
# MAGIC %sql
# MAGIC select x.addr_state,y.addr_state from db1.loan_risks_upload x inner join db1.loan_risks_upload Y on x.loan_id = y.loan_id where x.paid_amnt between 400 and 500

# COMMAND ----------

# DBTITLE 1,Clean up Parquet tables
# MAGIC %fs rm -r /tmp/flights_parquet 

# COMMAND ----------

# DBTITLE 1,Clean up Databricks Delta tables
# MAGIC %fs rm -r /tmp/flights_delta

# COMMAND ----------

# DBTITLE 1,Read CSV file into a dataframe
flights = spark.read.format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .load("/databricks-datasets/asa/airlines/2008.csv")

# COMMAND ----------

# DBTITLE 1,Write a Parquet based table using flights dataframe
flights.write.format("parquet").mode("overwrite").partitionBy("Origin").save("/tmp/flights_parquet")

# COMMAND ----------

# DBTITLE 1,Run a query against Parquet based table
from pyspark.sql.functions import count

flights_parquet = spark.read.format("parquet").load("/tmp/flights_parquet")

display(flights_parquet.filter("DayOfWeek = 1").groupBy("Month","Origin").agg(count("*").alias("TotalFlights")).orderBy("TotalFlights", ascending=False).limit(20))

# COMMAND ----------

# DBTITLE 1,Write a Databricks Delta based table using flights data
flights.write.format("delta").mode("overwrite").partitionBy("Origin").save("/tmp/flights_delta")

# COMMAND ----------

# DBTITLE 1,Run a query against Delta based table
flights_delta = spark.read.format("delta").load("/tmp/flights_delta")

display(flights_delta.filter("DayOfWeek = 1").groupBy("Month","Origin").agg(count("*").alias("TotalFlights")).orderBy("TotalFlights", ascending=False).limit(20))

# COMMAND ----------

# DBTITLE 1,Optimize the Delta table
display(spark.sql("DROP TABLE  IF EXISTS flights"))

display(spark.sql("CREATE TABLE flights USING DELTA LOCATION '/tmp/flights_delta'"))
                  
display(spark.sql("OPTIMIZE flights ZORDER BY (DayofWeek)"))


# COMMAND ----------

# DBTITLE 1,Run a query against Delta based table after optimization
flights_delta = spark.read.format("delta").load("/tmp/flights_delta")

display(flights_delta.filter("DayOfWeek = 1").groupBy("Month","Origin").agg(count("*").alias("TotalFlights")).orderBy("TotalFlights", ascending=False).limit(20))

# COMMAND ----------


