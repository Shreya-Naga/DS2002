#!/usr/bin/env python
# coding: utf-8

# import os 
# import numpy
# import pandas as pd
# from sqlalchemy import create_engine

# In[67]:


# import the necessary libraries
import os
import numpy
import pandas as pd
from sqlalchemy import create_engine


# In[114]:


# declare and assign connection variables for the MySql server and databases
host_name = "localhost"
host_ip = "127.0.0.1"
port = "3306"
user_id = "root"
pwd = "Papa0202"

src_dbname = "breastCancer"
dst_dbname = "breastCancer_dw"


# In[115]:


# define functions for getting data from and setting data into databases
def get_dataframe(user_id, pwd, host_name, db_name, sql_query):
    conn_str = f"mysql+pymysql://{user_id}:{pwd}@{host_name}/{db_name}"
    sqlEngine = create_engine(conn_str, pool_recycle=3600)
    connection = sqlEngine.connect()
    dframe = pd.read_sql(sql_query, connection);
    connection.close()
    
    return dframe


def set_dataframe(user_id, pwd, host_name, db_name, df, table_name, pk_column, db_operation):
    conn_str = f"mysql+pymysql://{user_id}:{pwd}@{host_name}/{db_name}"
    sqlEngine = create_engine(conn_str, pool_recycle=3600)
    connection = sqlEngine.connect()
    
    if db_operation == "insert":
        df.to_sql(table_name, con=connection, index=False, if_exists='replace')
        # sqlEngine.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({pk_column});")
            
    elif db_operation == "update":
        df.to_sql(table_name, con=connection, index=False, if_exists='append')
    
    connection.close()


# In[116]:


conn_str = f"mysql+pymysql://{user_id}:{pwd}@{host_name}"
sqlEngine = create_engine(conn_str, pool_recycle=3600)

sqlEngine.execute(f"DROP DATABASE IF EXISTS `{dst_dbname}`;")
sqlEngine.execute(f"CREATE DATABASE `{dst_dbname}`;")
sqlEngine.execute(f"USE {dst_dbname};")


# In[117]:


sql_wisconsin_data = "SELECT * FROM breastCancer.wisconsin;"
df_wisconsin_data = get_dataframe(user_id, pwd, host_name, src_dbname, sql_wisconsin_data)
df_wisconsin_data.head(2)


# In[118]:


sql_wdbc_data = "SELECT * FROM breastCancer.wdbc;"
df_wdbc_data = get_dataframe(user_id, pwd, host_name, src_dbname, sql_wdbc_data)
df_wdbc_data.head(2)


# In[119]:


drop_cols = ['index','5','1','2','3']
df_wisconsin_data.drop(drop_cols, axis=1, inplace=True)
df_wisconsin_data.rename(columns={"1000025":"patient_id", "1.1":"cell_size_uniformity", "1.2":"cell_shape_uniformity", "1.3":"cell_size", "1.4":"Bland Chromatin","1.5":"Normal Nucleoli", "2.1":"Mitoses"}, inplace=True)
df_wisconsin_data.head(2)


# In[120]:


drop_cols1 = ['0.1189','0.4601','0.2654','0.7119','0.6656','0.1622','2019','184.6','17.33','25.38','0.03003','0.006193']
df_wdbc_data.drop(drop_cols1, axis=1, inplace=True)
df_wdbc_data.rename(columns={"842302":"patient_id", "M":"diagnosis", "17.99":"radius", "10.38":"texture", "122.8":"Perimeter","1001":"Area", "0.1184":"Smoothness", "0.2776":"Compactness", "0.3001":"Concavity", "0.1471":"Symmetry", "0.2419":"Fractal_dimension", "0.07871":"mean_intensity", "1.095":"standard_error", "0.9053":"Worst_radius", "8.589":"worst_texture", "153.4":"worst_perimeter", "0.006399":"worst_area", "0.04904":"worst_smoothness", "0.05373":"worst_compactness", "0.01587":"worst_concavity"}, inplace=True)
df_wdbc_data.head(2)


# In[121]:


db_operation = "insert"

tables = [('dim_wisconsin_data', df_wisconsin_data, 'patient_id'),
         ('dim_wdbc_data', df_wdbc_data, 'patient_id')]


# In[122]:


for table_name, dataframe, primary_key in tables:
    set_dataframe(user_id, pwd, host_name, dst_dbname, dataframe, table_name, primary_key, db_operation)


# In[125]:


df_breastCancerFacts = pd.merge(df_wisconsin_data, df_wdbc_data, on='patient_id', how='right')
df_breastCancerFacts.head(2)


# In[127]:


table_name = "breastCancerFacts"
primary_key = "patient_id"
db_operation = "insert"

set_dataframe(user_id, pwd, host_name, dst_dbname, df_breastCancerFacts, table_name, primary_key, db_operation)


# sql_test = """
#     SELECT df_wisconsin.`patient_id` AS `patient_id`,
#         SUM(df_wdbc_data.`1001`) AS `total_cancerous_area`,
#         SUM(df_wisconsin_data.`cell_size`) AS `total_cell_size`
#     FROM `{0}`.`breastCancerFacts` AS df_wisconsin
#     INNER JOIN `{0}`.`breastCancerFacts` AS df_wdbc
#     ON orders.customer_id = customers.customer_key
#     GROUP BY customers.`last_name`
#     ORDER BY total_unit_prices DESC;
# """.format(dst_dbname)
# df_test = get_dataframe(user_id, pwd, host_name, src_dbname, sql_test)

# In[ ]:


sql_test = """ SELECT df_wisconsin.patient_id AS patient_id, SUM(df_wdbc_data.1001) AS total_cancerous_area, SUM(df_wisconsin_data.cell_size) AS total_cell_size FROM {0}.breastCancerFacts AS df_wisconsin INNER JOIN {0}.breastCancerFacts AS df_wdbc ON orders.customer_id = customers.customer_key GROUP BY customers.last_name ORDER BY total_unit_prices DESC; """.format(dst_dbname) df_test = get_dataframe(user_id, pwd, host_name, src_dbname, sql_test)


# In[143]:


df_test.head()


# In[144]:


df_breastCancerFacts.head(2)


# In[ ]:




