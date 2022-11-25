# Databricks notebook source
# MAGIC %md
# MAGIC Create a sample database, a sample table, load some data 

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
# MAGIC ) USING DELTA LOCATION '/mnt/deltalake/loan_risks_upload';

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO db1.loan_risks_upload
# MAGIC FROM '/databricks-datasets/learning-spark-v2/loans/loan-risks.snappy.parquet'
# MAGIC FILEFORMAT = PARQUET;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from db1.loan_risks_upload;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM db1.loan_risks_upload;

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
# MAGIC ) USING DELTA LOCATION '/mnt/deltalake/loan_risks_upload';
# MAGIC 
# MAGIC COPY INTO db1.loan_risks_upload
# MAGIC FROM '/databricks-datasets/learning-spark-v2/loans/loan-risks.snappy.parquet'
# MAGIC FILEFORMAT = PARQUET;
# MAGIC 
# MAGIC SELECT * FROM db1.loan_risks_upload;
# MAGIC 
# MAGIC -- Result:
# MAGIC -- +---------+-------------+-----------+------------+
# MAGIC -- | loan_id | funded_amnt | paid_amnt | addr_state |
# MAGIC -- +=========+=============+===========+============+
# MAGIC -- | 0       | 1000        | 182.22    | CA         |
# MAGIC -- +---------+-------------+-----------+------------+
# MAGIC -- | 1       | 1000        | 361.19    | WA         |
# MAGIC -- +---------+-------------+-----------+------------+
# MAGIC -- | 2       | 1000        | 176.26    | TX         |
# MAGIC -- +---------+-------------+-----------+------------+
# MAGIC -- ...

# COMMAND ----------

# MAGIC %md
# MAGIC Navigate to the container and folder in datalake gen 2 account and show the directory structure, data file and log file format

# COMMAND ----------

# DBTITLE 1,Basic Insert
# MAGIC %sql
# MAGIC Insert into db1.loan_risks_upload Select * from db1.loan_risks_upload

# COMMAND ----------

# DBTITLE 1,Read Data
# MAGIC %sql
# MAGIC Select count(*) from db1.loan_risks_upload

# COMMAND ----------

# DBTITLE 1,Select with Join
# MAGIC %sql
# MAGIC select x.addr_state,y.addr_state from db1.loan_risks_upload x inner join db1.loan_risks_upload Y on x.loan_id = y.loan_id where x.paid_amnt between 400 and 500
