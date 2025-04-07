# Databricks notebook source
# MAGIC %md
# MAGIC # TRANSFORMATIONS

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

df = spark.read.format('csv')\
    .option('inferSchema',True)\
    .option('header',True)\
    .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Command: SELECT

# COMMAND ----------

# MAGIC %md
# MAGIC **Way 1** 

# COMMAND ----------

df.select('Item_Identifier','Item_Weight','Item_Fat_Content').display()

# COMMAND ----------

df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Command: ALIAS

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Command: FILTER/WHERE <br>
# MAGIC **1. Filter the data with fat content wich is Regular.** <br>
# MAGIC **2. Slice the data with item type \= Soft Drinks and weight \< 10** <br>
# MAGIC **3. Fetch the data with Tier in (Tier 1 or Tier 2) and Outlet Size is Null**

# COMMAND ----------

df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

df.filter( (col('Item_Type') == 'Soft Drinks') & (col('Item_Weight')<10) ).display()

# COMMAND ----------


df.filter( (col('Outlet_Location_Type') == 'Tier 1') | (col('Outlet_Location_Type') == 'Tier 2') & (col('Outlet_Size').isNull())).display()



# COMMAND ----------

df.filter(col('Outlet_Location_Type') .isin('Tier 1','Tier 2') & (col('Outlet_Size').isNull())).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Command: withColumnRenamed
# MAGIC **Rename the name of the existing columns of dataframe**

# COMMAND ----------

df.withColumnRenamed('Item_Weight', 'Item_wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Command: withColumn
# MAGIC **It is used to add a new column to the dataframe by either creating a new column with constant using lit() or by doing some transformation or perfroming an aggregation and putting the result into new column, or by doing changes to the existing column**
# MAGIC
# MAGIC #### Scenario 1: Create a new column with constants

# COMMAND ----------

df = df.withColumn('flag',lit('new'))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 2: Create a new column with aggregation performed between two columns.

# COMMAND ----------

df.withColumn('multiply',col('Item_Weight')*col('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Scenario 3: Make changes to the exiting column such as change Item_Fat_Content column values to a new value such as <br>
# MAGIC ####1. Regualr to REG
# MAGIC ####2. Low Fat to LF

# COMMAND ----------

df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_content'),"Regular","REG"))\
    .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_content'),"Low Fat","LF")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Type Casting
# MAGIC **Changing the data type of columns**

# COMMAND ----------

df = df.withColumn('Item_Weight',col('Item_Weight').cast(StringType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Command: Sort/orderBy  (.sort() with asc() or desc())
# MAGIC **It is used to sort the dataframe with respect to one or more columns in either asceneding or descending order.**<br>
# MAGIC **By default when we use sort, it will sort in ascending order**

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Sorting based on multiple columns **
# MAGIC **Sort both column in ascending order**

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'],ascending=[0,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Sort first column in descending order and second column in ascending order**

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'],ascending=[0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Command: Limit

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

df.select('Item_Identifier','Item_Weight','Item_Fat_Content').limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Command: DROP - to drop single or multiple columns of dataframe

# COMMAND ----------

df.drop('Item_Visibility').display()

# COMMAND ----------

df.drop('Item_Visibility','Item_Type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Command: DROP_DUPLICATES

# COMMAND ----------

# MAGIC %md
# MAGIC Scn1: Removing the dupliacte rows/records from the entire dataframe.

# COMMAND ----------

df.display()

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario 2: Removing duplicated from a particular column

# COMMAND ----------

df.dropDuplicates(subset=['Item_Type']).display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Command: Union and Union by name

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparing Dataframes
# MAGIC

# COMMAND ----------

data1 = [ (1,'Kid'),
          (2,'Sid')]
schema1 = 'id string, name string'
df1 = spark.createDataFrame(data1,schema1)

# COMMAND ----------

data2 = [('3','Rahul'),
         ('4','Sayuj')]
schema2 = 'id string, name string'
df2 = spark.createDataFrame(data2,schema2)

# COMMAND ----------

df1.display()
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Perform union function on df1 and df2

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC Perfomr Union by name on df1 and df2
# MAGIC

# COMMAND ----------

data1 = [ ('Kid',1),
          ('Sid',2)]
schema1 = 'name string, id string'
df1 = spark.createDataFrame(data1,schema1)

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## String operations such as INITCAP(), LOWER(), UPPER()

# COMMAND ----------

# MAGIC %md
# MAGIC ### INITCAP()

# COMMAND ----------

df.select(initcap('Item_Type')).limit(5).display()

# COMMAND ----------

df.select(lower('Item_Type')).limit(5).display()

# COMMAND ----------

df.select(upper('Item_type').alias('Upper_Item_Type')).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date Functions such as current_date(), date_add(), date_sub()    

# COMMAND ----------

df = df.withColumn('curr_date',current_date())

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

df = df.withColumn('week_after',date_add('curr_date',7))
df.display()

# COMMAND ----------

df.withColumn('week_before',date_sub('curr_date',7)).limit(5).display()

# COMMAND ----------

df = df.withColumn('week_before',date_add('curr_date',-7))
df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATEDIFF to calculate the interval between two dates, say for an example number of days between the order purchased date and order delivery date.

# COMMAND ----------

df = df.withColumn('datediff',datediff('week_after','curr_date'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date_format function is used to change the format of the date value 

# COMMAND ----------

df.withColumn('week_before',date_format('week_before','dd-MM-yyyy')).limit(5).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Handling Nulls
# MAGIC **Handling Nulls is done with either droppping the records with Null values or filling the Null Values with some meaningful value.**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dropping records that contains null values using dropna()
# MAGIC dropna() function is used to delete the records that contain null values.<br>
# MAGIC There are mainly three options for dropna:<br>
# MAGIC 1. any - delete the record, if any of the column contain null values.<br>
# MAGIC 2. all - delet the record, if all the columns corresponding to the record has null values.<br>
# MAGIC 3. subset - where in column names are provides in which presence of null are noted, based on which records will be deleted.

# COMMAND ----------

df.dropna('all').display()

# COMMAND ----------

df.dropna('any').display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.dropna(subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filling records that contains null values using fillna()

# COMMAND ----------

df.fillna('NotAvailable').display()

# COMMAND ----------

df.fillna('Not Available',subset=['Outlet_Size']).display()

# COMMAND ----------

