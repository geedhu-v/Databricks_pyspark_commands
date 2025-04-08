# Databricks notebook source
# MAGIC %md
# MAGIC #Transformations Part 2

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.format('csv')\
    .option('inferSchema',True)\
    .option('header',True)\
    .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Split and Indexing 

# COMMAND ----------

df.withColumn('Outlet_Type',split(col('Outlet_Type'),' ')).display()

# COMMAND ----------

df.withColumn('Outlet_Type',split(col('Outlet_Type'),' ')[1]).limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explode function
# MAGIC It is a function that is used to the split the values in a list to separate values in the same column.

# COMMAND ----------

df_explode = df.withColumn('Outlet_Type',split('Outlet_Type',' '))
df_explode.limit(5).display()

# COMMAND ----------

df_explode.withColumn('Outlet_Type',explode('Outlet_Type')).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Array_contains
# MAGIC **It is a function to check for a particular value in a column that contains values in the form of a list/array.**

# COMMAND ----------

df_explode.withColumn('Type1_flag',array_contains('Outlet_Type',"Type1")).limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Group_by
# MAGIC **Go to functions for all the aggregation tasks.**

# COMMAND ----------

df.groupBy('Item_Type').sum('Item_MRP').display()

# COMMAND ----------

df.groupBy('Item_Type').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Perform groupby on more than one columns

# COMMAND ----------

df.groupBy(['Item_Type','Outlet_Size']).agg(sum('Item_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('total MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Performing group by with two columns and also perofrm aggregation on two columns.

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('total MRP'),avg('Item_MRP').alias('Avg_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Collect_List

# COMMAND ----------

# MAGIC %md 
# MAGIC **Prepare a dataframe to perofrm collect_list function.**

# COMMAND ----------

data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]
schema = 'user string, book string'

df_book = spark.createDataFrame(data,schema)

# COMMAND ----------

df_book.display()

# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function : PIVOT

# COMMAND ----------

df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## When-Otherwise is a command similar to the 'case-when' command in SQL.

# COMMAND ----------

df.withColumn('Veg_Flag',when(col('Item_Type')=="Meat","Non-Veg").otherwise('Veg')).limit(5).display()

# COMMAND ----------

df_veg_flag = df.withColumn('Veg_Flag',when(col('Item_Type')=="Meat","Non-Veg").otherwise('Veg'))

# COMMAND ----------

df_veg_flag.withColumn('Veg_Flag',when(((col('Veg_Flag')=='Veg') & (col('Item_MRP')>100)),'Veg Expensive')\
    .when(((col('Veg_Flag')=='Veg') & (col('Item_MRP')<100)),'Veg Inexpensive')\
    .otherwise('Veg Inexpensive')).display()