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


# In[7]:


# Azure SQL Server Connection Information #####################
jdbc_hostname = "ds-2002-mysql-sn.mysql.database.azure.com
"
jdbc_port = 1433
src_database = "AdventureWorksLT"

connection_properties = {
  "user" : "sn8kr",
  "password" : "hello123!",
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# MongoDB Atlas Connection Information ########################
atlas_cluster_name = "sandbox"
atlas_database_name = "adventure_works"
atlas_user_name = "m001-student"
atlas_password = "m001-mongodb-basics"

# Data Files (JSON) Information ###############################
dst_database = "adventure_works"

base_dir = "dbfs:/FileStore/ds3002-capstone"
database_dir = f"{base_dir}/{dst_database}"

data_dir = f"{base_dir}/source_data"
batch_dir = f"{data_dir}/batch"
stream_dir = f"{data_dir}/stream"

output_bronze = f"{database_dir}/fact_sales_orders/bronze"
output_silver = f"{database_dir}/fact_sales_orders/silver"
output_gold   = f"{database_dir}/fact_sales_orders/gold"

# Delete the Streaming Files ################################## 
dbutils.fs.rm(f"{database_dir}/fact_sales_orders", True)

# Delete the Database Files ###################################
dbutils.fs.rm(database_dir, True)


# In[8]:


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


# In[9]:


# create new databricks metadata database
get_ipython().run_line_magic('sql', '')
DROP DATABASE IF EXISTS adventure_works CASCADE;


# In[11]:


#create new databricks metadata database
get_ipython().run_line_magic('sql', '')
CREATE DATABASE IF NOT EXISTS adventure_works
COMMENT "Capstone Project Database"
LOCATION "dbfs:/FileStore/ds3002-capstone/adventure_works"
WITH DBPROPERTIES (contains_pii = true, purpose = "DS-3002 Capstone Project");
     


# In[12]:


# create new table that sources its data from a view in an azure sql database%sql
CREATE OR REPLACE TEMPORARY VIEW view_product
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:sqlserver://ds3002-sql.database.windows.net:1433;database=AdventureWorksLT",
  dbtable "SalesLT.vDimProducts",
  user "jtupitza",
  password "P@ssw0rd123"
)
     


# In[13]:


get_ipython().run_line_magic('sql', '')
USE DATABASE adventure_works;

CREATE TABLE IF NOT EXISTS adventure_works.dim_product
COMMENT "Products Dimension Table"
LOCATION "dbfs:/FileStore/ds3002-capstone/adventure_works/dim_product"
AS SELECT * FROM view_product


# In[14]:


get_ipython().run_line_magic('sql', '')
SELECT * FROM adventure_works.dim_product LIMIT 5


# In[15]:


get_ipython().run_line_magic('sql', '')
DESCRIBE EXTENDED adventure_works.dim_product;


# In[17]:


# create new table that sources its data from a table in an azure sql database
get_ipython().run_line_magic('sql', '')
CREATE OR REPLACE TEMPORARY VIEW view_date
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:sqlserver://ds3002-sql.database.windows.net:1433;database=AdventureWorksLT",
  dbtable "dbo.DimDate",
  user "sn8kr",
  password "hello123!"
)
     


# In[18]:


get_ipython().run_line_magic('sql', '')
USE DATABASE adventure_works;

CREATE TABLE IF NOT EXISTS adventure_works.dim_date
COMMENT "Date Dimension Table"
LOCATION "dbfs:/FileStore/ds3002-capstone/adventure_works/dim_date"
AS SELECT * FROM view_date


# In[19]:


get_ipython().run_line_magic('sql', '')
SELECT * FROM adventure_works.dim_date LIMIT 5


# In[20]:


get_ipython().run_line_magic('sql', '')
DESCRIBE EXTENDED adventure_works.dim_date;


# In[21]:


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


# In[ ]:


# use pyspark to read from a csv file
address_csv = f"{batch_dir}/AdventureWorksLT_DimAddress.csv"

df_address = spark.read.format('csv').options(header='true', inferSchema='true').load(address_csv)
display(df_address)

