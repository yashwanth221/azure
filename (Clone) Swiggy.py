# Databricks notebook source
dbutils.fs.ls("/mnt/raw_data/swiggy.csv")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, lit, desc, split, avg, sum, max, min, count, collect_list, collect_set, explode, flatten,broadcast, udf, to_date, row_number, rank, sum, dense_rank, lead, lag, date_format, round, month, avg, initcap, lower, substring, regexp_extract, regexp_replace, isnull, add_months, date_add, date_sub,dayofweek,dayofmonth,dayofyear,current_timestamp, current_date
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DoubleType, IntegerType
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import current_date, current_timestamp, hour, minute, second, col, day, month, year
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("File_name","")
dbutils.widgets.text("Last_Success_date","")
dbutils.widgets.text("config_id","")
file_path = dbutils.widgets.get("File_name")
LS_date = dbutils.widgets.get("Last_Success_date")
c_id = dbutils.widgets.get("config_id")
print(file_path)
print(LS_date)
print(c_id)

# COMMAND ----------

df = spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimeter",",").load("/mnt/raw_data/swiggy.csv")
df1 = df.dropDuplicates(["SNo","OID","CustomerID"])

df1 = df1.withColumnRenamed("Item Type","Item_Type").withColumnRenamed("Total Cost","Total_Cost").withColumnRenamed("Unit Cost","Unit_Cost").withColumnRenamed("Payment Type","Payment_Type")
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists practise

# COMMAND ----------


df1.write.mode("append").saveAsTable("practise.swiggy")

# COMMAND ----------

# Define the connection string
# JDBC connection details
jdbc_url = "jdbc:sqlserver://medishetty-server.database.windows.net:1433;database=medishetty_database;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
username = 'Admin02'
password = 'Admin@02'

connection_properties = {
    "user": 'Admin02',
    "password": 'Admin@02',
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# update data to the staging table
df1.write.jdbc(url=jdbc_url, table="dbo.stg_swiggy", mode ="append", properties=connection_properties)

# COMMAND ----------

# Your merge query
sql_merge_query = """
merge into fact_swiggy tgt using stg_swiggy stg 
on tgt.O_ID = stg.OID and tgt.Customer_ID = stg.CustomerID

when matched then
update set	SNo = stg.SNo, Date = stg.Date, Location= stg.Location,	Item = stg.Item,	Item_Type = stg.Item_Type, Quantity = stg.Quantity, 	Unit_Cost = stg.Unit_Cost,	Total_Cost = stg.Total_Cost,	Payment_Type = stg.Payment_Type,	Rating = stg.Rating,	Restaurant= stg.Restaurant

when not matched by target then
insert (SNo, O_ID,	Customer_ID,	Date,	Location,	Item,	Item_Type,	Quantity,	Unit_Cost,	Total_Cost,	Payment_Type,	Rating,	Restaurant)
values (SNo, stg.OID,	stg.CustomerID,	stg.Date,	stg.Location,	stg.Item,	stg.Item_Type,	stg.Quantity,	stg.Unit_Cost,	stg.Total_Cost,	stg.Payment_Type,	stg.Rating,	stg.Restaurant);
"""

# Run the SQL using JDBC from JVM
conn = spark._sc._jvm.java.sql.DriverManager.getConnection(jdbc_url, username, password)
stmt = conn.createStatement()
stmt.execute(sql_merge_query)



# COMMAND ----------

# to update master config table in sql
sql_update_master_config = f"""update master_config set notebook_last_success_date = current_timestamp where config_id = '{c_id}'"""

# Run the SQL using JDBC from JVM
conn = spark._sc._jvm.java.sql.DriverManager.getConnection(jdbc_url, username, password)
stmt = conn.createStatement()
stmt.execute(sql_update_master_config,)
stmt.close()
conn.close()

