#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip install pyspark')


# In[5]:


get_ipython().system('pip install pyarrow')


# In[6]:


# import necessary libraries
import os
import json
import pymongo
import pyspark.pandas as pd  # This uses Koalas that is included in PySpark version 3.2 or newer.
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BinaryType
from pyspark.sql.types import ByteType, ShortType, IntegerType, LongType, FloatType, DecimalType


# In[37]:


# Azure SQL Server Connection Information #####################
jdbc_hostname = "ds-2002-mysql-sn.mysql.database.azure.com"
jdbc_port = 1433
src_database = "breastCancer-azure"

connection_properties = {
  "user" : "sn8kr",
  "password" : "hello123!",
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# MongoDB Atlas Connection Information ########################
atlas_cluster_name = "sandbox"
atlas_database_name = "breastcancer-azure"
atlas_user_name = "m001-student"
atlas_password = "m001-mongodb-basics"

# Data Files (JSON) Information ###############################
dst_database = "breastcancer-azure"

base_dir = "dbfs:/FileStore/ds-2002-mysql-sn"
database_dir = f"{base_dir}/{dst_database}"

data_dir = f"{base_dir}/source_data"
batch_dir = f"{data_dir}/batch"
stream_dir = f"{data_dir}/stream"

output_bronze = f"{database_dir}/fact_sales_orders/bronze"
output_silver = f"{database_dir}/fact_sales_orders/silver"
output_gold   = f"{database_dir}/fact_sales_orders/gold"

# Delete the Streaming Files ################################## 
#dbutils.fs.rm(f"{database_dir}/fact_sales_orders", True)

# Delete the Database Files ###################################
#dbutils.fs.rm(database_dir, True)


# In[38]:


# ######################################################################################################################
# Use this Function to Fetch a DataFrame from the Azure SQL database server.
# ######################################################################################################################
def get_sql_dataframe(host_name, port, db_name, conn_props, sql_query):
    '''Create a JDBC URL to the Azure SQL Database'''
    jdbcUrl = f"jdbc:sqlserver://{host_name}:{port};database={db_name}"
    
    '''Invoke the spark.read.jdbc() function to query the database, and fill a Pandas DataFrame.'''
    dframe = spark.read.jdbc(url=jdbcUrl, table=sql_query, properties=conn_props)
    
    return dframe


# ######################################################################################################################
# Use this Function to Fetch a DataFrame from the MongoDB Atlas database server Using PyMongo.
# ######################################################################################################################
def get_mongo_dataframe(user_id, pwd, cluster_name, db_name, collection, conditions, projection, sort):
    '''Create a client connection to MongoDB'''
    mongo_uri = f"mongodb+srv://{user_id}:{pwd}@{cluster_name}.zibbf.mongodb.net/{db_name}?retryWrites=true&w=majority"
    
    client = pymongo.MongoClient(mongo_uri)

    '''Query MongoDB, and fill a python list with documents to create a DataFrame'''
    db = client[db_name]
    if conditions and projection and sort:
        dframe = pd.DataFrame(list(db[collection].find(conditions, projection).sort(sort)))
    elif conditions and projection and not sort:
        dframe = pd.DataFrame(list(db[collection].find(conditions, projection)))
    else:
        dframe = pd.DataFrame(list(db[collection].find()))

    client.close()
    
    return dframe

# ######################################################################################################################
# Use this Function to Create New Collections by Uploading JSON file(s) to the MongoDB Atlas server.
# ######################################################################################################################
def set_mongo_collection(user_id, pwd, cluster_name, db_name, src_file_path, json_files):
    '''Create a client connection to MongoDB'''
    mongo_uri = f"mongodb+srv://{user_id}:{pwd}@{cluster_name}.zibbf.mongodb.net/{db_name}?retryWrites=true&w=majority"
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]
    
    '''Read in a JSON file, and Use It to Create a New Collection'''
    for file in json_files:
        db.drop_collection(file)
        json_file = os.path.join(src_file_path, json_files[file])
        with open(json_file, 'r') as openfile:
            json_object = json.load(openfile)
            file = db[file]
            result = file.insert_many(json_object)

    client.close()
    
    return result


# In[39]:


# create new databricks metadata database
get_ipython().run_line_magic('sql', '')
DROP DATABASE IF EXISTS breastCancer-azure CASCADE;


# In[40]:


#create new databricks metadata database
get_ipython().run_line_magic('sql', '')
CREATE DATABASE IF NOT EXISTS adventure_works
COMMENT "Capstone Project Database"
LOCATION "dbfs:/FileStore/ds-2002-mysql-sn/breastCancer_azure"
WITH DBPROPERTIES (contains_pii = true, purpose = "DS-2002 Capstone Project");
     


# In[12]:


# create new table that sources its data from a view in an azure sql database%sql
CREATE OR REPLACE TEMPORARY VIEW view_product
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:sqlserver://ds-2002-mysql-sn.mysql.database.azure.com:1433;database=breastcancer-azure",
  dbtable "SalesLT.vDimProducts",
  user "sn8kr",
  password "hello123!"
)
     


# In[41]:


get_ipython().run_line_magic('sql', '')
USE DATABASE breastcancer-azure;

CREATE TABLE IF NOT EXISTS breastcancer-azure.dim-wisconsin
COMMENT "Wisconsin Dimension Table"
LOCATION "dbfs:/FileStore/ds-2002-mysql-sn/breastcancer-azure/dim-wisconsin"
AS SELECT * FROM view_wisconsin


# In[14]:


get_ipython().run_line_magic('sql', '')
SELECT * FROM breastCancer-azure.dim-wisconsin LIMIT 5


# In[15]:


get_ipython().run_line_magic('sql', '')
DESCRIBE EXTENDED breastcancer-azure.dim_wisconsin;


# In[17]:


# create new table that sources its data from a table in an azure sql database
get_ipython().run_line_magic('sql', '')
CREATE OR REPLACE TEMPORARY VIEW view_date
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:sqlserver://ds-2002-mysql-sn.mysql.database.azure.comt\:1433;database=breastcancer-azure",
  dbtable "dbo.DimDate",
  user "sn8kr",
  password "hello123!"
)
     


# In[18]:


get_ipython().run_line_magic('sql', '')
USE DATABASE breastcancer-azure;

CREATE TABLE IF NOT EXISTS breastcancer-azure.dim_wdbc
COMMENT "WDBC Dimension Table"
LOCATION "dbfs:/FileStore/ds3002-capstone/breastcancer-azure/dim_wdbc"
AS SELECT * FROM patient_id


# In[42]:


get_ipython().run_line_magic('sql', '')
SELECT * FROM breastcancer-azure.dim_wdbc LIMIT 5


# In[43]:


get_ipython().run_line_magic('sql', '')
DESCRIBE EXTENDED breastcancer-azure.dim_wdbc;


# In[44]:


# view the data files on teh databricks file system
display(dbutils.fs.ls(batch_dir))


# In[22]:


# create a new mongoDb database and load json data into a new modoDB collection
source_dir = '/dbfs/FileStore/ds3002-capstone/source_data/batch'
json_files = {"customers" : 'AdventureWorksLT_DimCustomer.json'}
set_mongo_collection(atlas_user_name, atlas_password, atlas_cluster_name, atlas


# In[23]:


# fetch data from teh new mongoDB collection
get_ipython().run_line_magic('scala', '')
import com.mongodb.spark._

val df_customer = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "adventure_works").option("collection", "customers").load()
display(df_customer)


# In[24]:


get_ipython().run_line_magic('scala', '')
df_customer.printSchema()


# In[25]:


# use spark datafram to create a new table in the databricks metadata database
get_ipython().run_line_magic('scala', '')
df_customer.write.format("delta").mode("overwrite").saveAsTable("adventure_work


# In[26]:


get_ipython().run_line_magic('sql', '')
DESCRIBE EXTENDED adventure_works.dim_customer


# In[27]:


#query the new table in the databricks metadata database
get_ipython().run_line_magic('sql', '')
SELECT * FROM adventure_works.dim_customer LIMIT 5


# In[45]:


# use pyspark to read from a csv file
address_csv = f"{batch_dir}/breast-cancer-wisconsin.csv"

df_address = spark.read.format('csv').options(header='true', inferSchema='true').load(address_csv)
display(df_address)


# In[46]:


get_ipython().run_line_magic('sql', '')
SELECT * FROM breastcancer-azure.dim_ LIMIT 5;


# In[47]:


#verify the dimension tables
get_ipython().run_line_magic('sql', '')
USE adventure_works;
SHOW TABLES


# In[48]:


# use autoloader to process streaming data and perform aggregations
(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 .option("cloudFiles.schemaHints", "SalesOrderID INT")
 .option("cloudFiles.schemaHints", "RevisionNumber TINYINT")
 .option("cloudFiles.schemaHints", "OrderDate TIMESTAMP")
 .option("cloudFiles.schemaHints", "DueDate TIMESTAMP") 
 .option("cloudFiles.schemaHints", "ShipDate TIMESTAMP")
 .option("cloudFiles.schemaHints", "Status TINYINT")
 .option("cloudFiles.schemaHints", "OnlineOrderFlag BINARY")
 .option("cloudFiles.schemaHints", "SalesOrderNumber STRING")
 .option("cloudFiles.schemaHints", "PurchaseOrderNumber STRING") 
 .option("cloudFiles.schemaHints", "AccountNumber STRING")
 .option("cloudFiles.schemaHints", "CustomerID INT")
 .option("cloudFiles.schemaHints", "ShipToAddressID INT")
 .option("cloudFiles.schemaHints", "BillToAddressID INT")
 .option("cloudFiles.schemaHints", "ShipMethod STRING")
 .option("cloudFiles.schemaHints", "SubTotal FLOAT")
 .option("cloudFiles.schemaHints", "TaxAmt FLOAT")
 .option("cloudFiles.schemaHints", "Freight FLOAT")
 .option("cloudFiles.schemaHints", "TotalDue FLOAT")
 .option("cloudFiles.schemaHints", "SalesOrderDetailID INT")
 .option("cloudFiles.schemaHints", "OrderQty SMALLINT")
 .option("cloudFiles.schemaHints", "ProductID INT")
 .option("cloudFiles.schemaHints", "UnitPrice FLOAT")
 .option("cloudFiles.schemaHints", "UnitPriceDiscount FLOAT")
 .option("cloudFiles.schemaHints", "LineTotal DECIMAL")
 .option("cloudFiles.schemaHints", "rowguid STRING")
 .option("cloudFiles.schemaHints", "ModifiedDate TIMESTAMP")
 .option("cloudFiles.schemaLocation", output_bronze)
 .option("cloudFiles.inferColumnTypes", "true")
 .option("multiLine", "true")
 .load(stream_dir)
 .createOrReplaceTempView("orders_raw_tempview"))


# In[49]:


get_ipython().run_line_magic('sql', '')
*(Add, Metadata, for, Traceability, */)
CREATE OR REPLACE TEMPORARY VIEW orders_bronze_tempview AS (
  SELECT *, current_timestamp() receipt_time, input_file_name() source_file
  FROM orders_raw_tempview
)
     


# In[50]:



get_ipython().run_line_magic('sql', '')
DESCRIBE EXTENDED orders_silver_tempview
get_ipython().run_line_magic('sql', '')
CREATE OR REPLACE TEMPORARY VIEW fact_orders_silver_tempview AS (
  SELECT t.SalesOrderID
    , t.RevisionNumber
    , od.MonthName AS OrderMonth
    , od.WeekDayName AS OrderDayName
    , od.Day AS OrderDay
    , od.Year AS OrderYear
    , dd.MonthName AS DueMonth
    , dd.WeekDayName AS DueDayName
    , dd.Day AS DueDate
    , dd.Year AS DueYear
    , sd.MonthName AS ShipMonth
    , sd.WeekDayName AS ShipDayName
    , sd.Day AS ShipDay
    , sd.Year AS ShipYear
    , t.Status
    , t.OnlineOrderFlag
    , t.SalesOrderNumber
    , t.PurchaseOrderNumber
    , t.AccountNumber
    , c.CustomerID
    , c.FirstName
    , c.LastName
    , t.ShipToAddressID
    , sa.AddressLine1 AS ShipToAddressLine1
    , sa.AddressLine2 AS ShipToAddressLine2
    , sa.City AS ShipToCity
    , sa.StateProvince AS ShipToStateProvince
    , sa.PostalCode AS ShipToPostalCode
    , t.BillToAddressID
    , ba.AddressLine1 AS BillToAddressLine1
    , ba.AddressLine2 AS BillToAddressLine2
    , ba.City AS BillToCity
    , ba.StateProvince AS BillToStateProvince
    , ba.PostalCode AS BillToPostalCode
    , t.ShipMethod
    , t.SubTotal
    , t.TaxAmt
    , t.Freight
    , t.TotalDue
    , t.SalesOrderDetailID
    , t.OrderQty
    , p.ProductID
    , p.ProductNumber
    , t.UnitPrice
    , t.UnitPriceDiscount
    , t.LineTotal
    , t.rowguid
    , t.ModifiedDate
    , t.receipt_time
    , t.source_file
  FROM orders_silver_tempview t
  INNER JOIN adventure_works.dim_customer c
  ON t.CustomerID = c.CustomerID
  INNER JOIN adventure_works.dim_address sa
  ON t.ShipToAddressID = CAST(sa.AddressID AS BIGINT)
  INNER JOIN adventure_works.dim_address ba
  ON t.BillToAddressID = CAST(ba.AddressID AS BIGINT)
  INNER JOIN adventure_works.dim_product p
  ON t.ProductID = p.ProductID
  INNER JOIN adventure_works.dim_date od
  ON CAST(t.OrderDate AS DATE) = od.Date
  INNER JOIN adventure_works.dim_date dd
  ON CAST(t.DueDate AS DATE) = dd.Date
  INNER JOIN adventure_works.dim_date sd
  ON CAST(t.ShipDate AS DATE) = sd.Date)
     

(spark.table("fact_orders_silver_tempview")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{output_silver}/_checkpoint")
      .outputMode("append")
      .table("fact_orders_silver"))
     
get_ipython().run_line_magic('sql', '')
SELECT * FROM fact_orders_silver
     
get_ipython().run_line_magic('sql', '')
DESCRIBE EXTENDED adventure_works.fact_orders_silver


# In[51]:


get_ipython().run_line_magic('sql', '')
SELECT CustomerID
  LastName("")
  FirstName("")
  OrderMonth("")
  COUNT("(ProductID)", "AS", "ProductCount")
FROM adventure_works.fact_orders_silver
GROUP BY CustomerID, LastName, FirstName, OrderMonth
ORDER BY ProductCount DESC


# In[ ]:




