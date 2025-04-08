# Databricks_pyspark_commands

## Select command
As the name suggest the select particular columns from the entire dataframe. <br>
Syntax: dataframe_name.select(col(col1),col(col2))

## Alias command
It is used to display an existing column name with some other name for our easy understanding.<br>
Syntax: dataframe_name.select(col(col1_oldName).alias(col1_newName)
<img width="362" alt="image" src="https://github.com/user-attachments/assets/3ce8549c-5580-4e13-b3ff-44fa5f42b853" />


## Filter command
It is used to display ceratin elemnets from the entire dataset based on one or more conditions. <br>
Syntax: <br>
With single condition: dataframe_name.filter(col(col1_name)=='Value') <br>
With more than one condition: dataframe_name.filter((col(col1_name)=='Value') & (col(col2_name)=='Value')) <br>

## withColumnRenamed command
It is used the rename the column name of a dataframe. <br>
Syntax: dataframe_name.withColumnRenamed('Old_Name','New_Name')

## withColumn command
It is a command that is used to either create a new column with constants/calculate any value or modify an existing column. <br>
Syntax:<br>
dataframe_name.withColumn('new_col_name','value to be displayed')

<img width="914" alt="image" src="https://github.com/user-attachments/assets/90987140-b045-4f94-bb5a-7f50d3ee3f6f" />

<img width="927" alt="image" src="https://github.com/user-attachments/assets/bbf26f7e-d76a-43d0-bbfa-a9686a409481" />

<img width="903" alt="image" src="https://github.com/user-attachments/assets/d254ced2-d86e-4265-b369-b76e19b309be" />


#### Command commands to work with: isin(), isNull(),is NotNull(), regexp_replace(col(col_name),'Old_value','New_value')

## Type casting
It is a command used the change the datatype of col values.
dataframe_name.withColumn('col_name',col('Col_name').cast(stringType()))

<img width="945" alt="image" src="https://github.com/user-attachments/assets/5a723fe9-9445-40cc-9a21-a8a8cd1dbc6c" />


## Command: Sort/orderBy  (.sort() with asc() or desc())
It is used to sort the dataframe with respect to one or more columns in either asceneding or descending order.<br>
**By default when we use sort, it will sort in ascending order**<br>
Syntax: <br>
1. dataframe_name.sort(col('col_name').desc())<br>
<img width="915" alt="image" src="https://github.com/user-attachments/assets/a419eeb9-2696-48c9-9eb8-7596fa426657" />

2. dataframe_name.sort(['col1_name','col2_name'],ascending=[0,0]) -> Meaning bothe columns are sorted in descending order.
  <img width="926" alt="image" src="https://github.com/user-attachments/assets/ed9a2c21-1ef2-4d9f-9e28-ee43da6bc077" />


## Command: Limit
To limit the number of records while displaying.<br>
Syntax: dataframe_name.limit(Value)

## Command: Drop
To drop single or multiple columns of a dataframe<br>
Syntax: dataframe_name.drop('col1_name','col2_name')

## Command: Drop duplicates
To drop the duplicates from either the enitire dataframe row-wise or column-wise or from a subset of columns present in the dataframe.<br>
Syntax: <br>
1. Drop the duplicates from the dataframe row-wise<br>
   dataframe_name.dropDuplicates()<br>
2. Drop the duplicates from the dataframe column-wise<br>
   dataframe_name.distinct()<br>
3. Drop the duplicates from the subset of columns from the dataframe<br>
   dataframe_name.dropDuplicates(subset=['col1_name','col2_name'])

## Command: Union
Syntax: dataframe1_name.union(dataframe2_name)<br>
<img width="176" alt="image" src="https://github.com/user-attachments/assets/4eb6064a-bfed-4d6b-9d08-7e19bf35cdf1" />

## Command by name
Syntax: dataframe1_name.unionByName(dataframe2_name)<br>
<img width="200" alt="image" src="https://github.com/user-attachments/assets/f6870dc8-a6e0-4634-892a-b4c3f3c7c4d6" />

## String Functtions such as inticap(), lower() and upper()
Syntax: dataframe_name.lower('column_name') / dataframe_name.upper('column_name')/ dataframe_name.initcap('column_name')

## Date Functions such as current_date(), date_add(), date_sub()
Syntax: dataframe.withColumn('new_column_name',current_date()) / <br>
dataframe.withColumn('new_column_name',date_add('existing_column_name',number_of_days)) / <br>
dataframe.withColumn('new_column_name',date_add('existing_column_name',number_of_days))<br>

## DateDiff function used to calculate the interval between two dates, , say for an example number of days between the order purchased date and order delivery date.
Syntax: dataframe_name.withColumn('new_column_name',datediff('end_date','start_date'))

## Date_format function is used to change the format of the date value 
Changing the date value from format yyyy-MM-dd to dd-MM-yyyy <br>
Syntax: dataframe_name.withColumn('existing_column_name',date_format('existing_column_name','format in string'))

## Handling Nulls
Handling Nulls is done with either droppping the records with Null values or filling the Null Values with some meaningful value.<br>

#### 1. Dropping Records that contains null values
dropna() function is used to delete the records that contain null values.<br>
There are mainly two options for dropna:<br>
1. any - delete the record, if any of the column contain null values.<br>
Syntax : dataframe_name.dropna('any') <br>
2. all - delet the record, if all the columns corresponding to the record has null values.<br>
Syntax: dataframe_name.dropna('all') <br>
3. subset - where in column names are provided in which presence of null are noted and based on which records will be deleted.
Syntax: dataframe_name.dropna(subset=['column_name'])

#### 2. Filling Nulls
fillna() function is used to fill the nullvalues in a record in the below shown two ways: <br>
1. all<br>
Syntax: dataframe_name.fillna('Value in string to replace the null value','all') <br>
2. subset <br>
Syntax: dataframe_name.fillna('Value in string to replace the null value',subset=['col1_name','col2_name'])

## Split and Indexing
Split function, to split the string based on delimiters to a list.
#### 1. Split 
<img width="944" alt="image" src="https://github.com/user-attachments/assets/6fd4575d-d6ff-4e30-9141-7e7d180d1253" />
#### 2. Split with Indexing
<img width="892" alt="image" src="https://github.com/user-attachments/assets/51207953-15e6-4375-b405-b7204fb4d835" />

## Explode function
It is a function, used to split the values in a list to separate values in the same column.
<img width="896" alt="image" src="https://github.com/user-attachments/assets/21b1adcf-1c40-4077-9a71-9e82fb86ee8f" />
<img width="143" alt="image" src="https://github.com/user-attachments/assets/ca5d993d-9801-48df-a102-1074af66b6ef" />

## Array_contains
It is a function to check for a particular value in a column that contains values in the form of a list/array.
<img width="901" alt="image" src="https://github.com/user-attachments/assets/5581a935-b492-4650-bbca-214a3ee38941" />

## Group_by
Go to functions for all the aggregation tasks.
<img width="923" alt="image" src="https://github.com/user-attachments/assets/a93b1e0c-664b-45d2-924d-246692b14372" />
#### Performing group by with two columns and also perofrm aggregation on two columns.
<img width="894" alt="image" src="https://github.com/user-attachments/assets/2b032349-c95a-4b98-a879-8db4e09149ff" />

## Collect_List
<img width="764" alt="image" src="https://github.com/user-attachments/assets/7720ce7c-ba32-463c-a79e-af4625667fa9" />
#### Preparing dataframe
<img width="920" alt="image" src="https://github.com/user-attachments/assets/fc4263ff-1110-4249-a6d8-40e5d314d637" />
#### Performing Collect_list function
<img width="915" alt="image" src="https://github.com/user-attachments/assets/d6a2a434-eee7-4ae3-92a8-d4d64009e05e" />

## Pivot
dataframe_name.groupBy('Column_name_which will be the Row value of pivot table').pivot('Column_name_which will be the Column value of Pivot table').agg(avg(column_name whose value to be aggrgated))
<img width="545" alt="image" src="https://github.com/user-attachments/assets/e2c93629-aa62-4172-a8b7-0f2302bda8e7" />

## When-Otherwise is a command similar to the 'case-when' command in SQL.

<img width="919" alt="image" src="https://github.com/user-attachments/assets/ef12e9ba-5c60-4964-8a72-5aa581daf215" />

#### Performing nested when otherwise conditions
<img width="915" alt="image" src="https://github.com/user-attachments/assets/b04046d2-9df8-4862-9f45-5f7a060ddd38" />



































