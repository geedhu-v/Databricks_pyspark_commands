# User Defined Functions (UDF)
#### To create a UDF, follow the below steps
<br> 1. Create a normal Python function using the 'def' function.
<img width="185" alt="image" src="https://github.com/user-attachments/assets/bd49472f-089e-4e05-94c9-3f168c11fa9d" />
<br> 2. Create a pyspark function with the normal Python function using the 'udf' function.
<img width="157" alt="image" src="https://github.com/user-attachments/assets/b6a304a0-6230-4b8f-bed1-96149eea1507" />
<br> 2. Use the pyspark function.
<img width="897" alt="image" src="https://github.com/user-attachments/assets/969c5f45-d4cc-4234-a4ec-e2a924ff42f5" />

##  Modes of Data Writting:
**1. Append** - If any file exits already in the location new file will be added right below the exitsing ones.
<br> Note : if new file has same name as existing file name, then the new file will not be appended.
<img width="923" alt="image" src="https://github.com/user-attachments/assets/8baea0b9-ed81-4275-86be-f5f803a58415" />

<br> **2. Overwrite** - If a file exist in the location, it will delete the existing file and insert the new file. Used in Staging purpose.
<img width="365" alt="image" src="https://github.com/user-attachments/assets/a3fa7209-0454-4d02-94a5-633971c3a37a" />

<br> **3. Ignore** - If a file exists in the location, it will not throw any error also will not insert any new file.
<img width="599" alt="image" src="https://github.com/user-attachments/assets/2abbb485-f208-4365-95ee-2c48a610a4fe" />

<br> **4. Error** - if a file exist in the location, it will throw error and will not insert any file.
<img width="452" alt="image" src="https://github.com/user-attachments/assets/96a7d3af-31c8-4744-b1fa-b9cf9391c6b1" />

## Writting the data in Parquet
<img width="637" alt="image" src="https://github.com/user-attachments/assets/b5cbd8d7-6a71-46cf-b335-8355470d16ac" />

## Spark SQL
<img width="921" alt="image" src="https://github.com/user-attachments/assets/5ec78a98-9fcb-413b-a50e-58954496cd71" />
<img width="913" alt="image" src="https://github.com/user-attachments/assets/d34580dc-d4db-40d5-8dd9-881c29649e0c" />





  
