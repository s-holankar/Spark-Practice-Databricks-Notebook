# Databricks notebook source
dataDF = [(('James','','Smith'),'1991-04-01','M',3000),
  (('Michael','Rose',''),'2000-05-19','M',4000),
  (('Robert','','Williams'),'1978-09-05','M',4000),
  (('Maria','Anne','Jones'),'1967-12-01','F',4000),
  (('Jen','Mary','Brown'),'1980-02-17','F',-1)
]

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
schema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('dob', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])

# COMMAND ----------

df = spark.createDataFrame(data = dataDF, schema = schema)
df.printSchema()

# COMMAND ----------

df.withColumnRenamed("dob","DateOfBirth").printSchema()

# COMMAND ----------

df.show()

# COMMAND ----------

df2 = df.withColumnRenamed("dob","DateOfBirth") \
        .withColumnRenamed("salary","salary_amount")
df2.printSchema()

# COMMAND ----------

df2.show()

# COMMAND ----------


schema2 = StructType([
    StructField("fname",StringType()),
    StructField("middlename",StringType()),
    StructField("lname",StringType())])

# COMMAND ----------

newColumns = ["newCol1","newCol2","newCol3","newCol4"]
df.toDF(*newColumns).printSchema()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField 
from pyspark.sql.types import StringType, IntegerType, ArrayType

# Create SparkSession object
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# Create data
data = [
    (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
    (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
    (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
    (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
    (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
    (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
 ]

# Create schema        
schema = StructType([
     StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
     ])),
     StructField('languages', ArrayType(StringType()), True),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
 ])

# Create dataframe
df = spark.createDataFrame(data = data, schema = schema)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

df.filter(df.state == 'OH').show(truncate=False)

# COMMAND ----------

# Not equals condition
df.filter(df.state != "OH") \
    .show(truncate=False) 

# Another expression
df.filter(~(df.state == "OH")) \
    .show(truncate=False)

# COMMAND ----------

# MAGIC %md *** Window Functions ***

# COMMAND ----------

simpleData = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
  )
 
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import row_number, col
from pyspark.sql.window import Window
window = Window.partitionBy("department").orderBy(col("salary").desc())

df_with_row = df.withColumn("row",row_number().over(window))
display(df_with_row)

# COMMAND ----------

from pyspark.sql.functions import rank
df.withColumn("rank",rank().over(window)).show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import dense_rank
df.withColumn("dense_rank",dense_rank().over(window)).show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import lead
df.withColumn("lead",lead("salary",2).over(window)).show(truncate=False)

# COMMAND ----------

data = [ ['2024-01-01',20000], ['2024-01-02',10000],[ '2024-01-03',150000], ['2024-01-04',100000], ['2024-01-05',210000]] 
  
#define column names
columns = ['date', 'sales'] 
  
#create dataframe using data and column names
df = spark.createDataFrame(data, columns) 
display(df)

# COMMAND ----------

df.createTempView('sample')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sample

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,sum(sales) over(order by date asc)cum_sales,
# MAGIC lag(sales) over(order by date asc) as prev_sales,
# MAGIC lead(sales) over (order by date asc) as next_sales
# MAGIC from sample

# COMMAND ----------

from pyspark.sql.functions import sum
from pyspark.sql.window import Window

w = Window.orderBy('date')

new_df = df.withColumn('cum_sales',sum('sales').over(w))
new_df.show()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
data = [
    ("2024-09-01", 1, 101, 10),
    ("2024-09-02", 1, 101, 20),
    ("2024-09-03", 1, 101, 15),
    ("2024-09-04", 1, 101, 25),
    ("2024-09-05", 1, 101, 30),
    ("2024-09-06", 1, 102, 35),
    ("2024-09-07", 2, 101, 40),
    ("2024-09-08", 2, 101, 45),
    ("2024-09-01", 2, 102, 5)]
	
	
schema = StructType([
         StructField("date", StringType() , True),
		 StructField("store_id", IntegerType(), True),
		 StructField("product_id", IntegerType(), True),
		 StructField("sales", IntegerType(), True)
])
		 

df = spark.createDataFrame(data, schema=schema)

df.show()

# COMMAND ----------

# MAGIC %md ### Your task is to calculate the cumulative sales for each product in each store, ordered by date.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

window = Window.partitionBy("product_id","store_id").orderBy(col("date"))

df_with_row = df.withColumn("cum_sales",sum("sales").over(window))

df_with_row.show()

# COMMAND ----------

# MAGIC %md ### find out missing 

# COMMAND ----------

data=[(1,),(2,),(5,),(7,),(8,),(10,)]
column=['ID']
df=spark.createDataFrame(data,column)
df.show()

# COMMAND ----------

list = range(1,11,1)
df_new = spark.createDataFrame(list, IntegerType())
df_new.show()

# COMMAND ----------

df_new = df_new.subtract(df)
df_new.show()

# COMMAND ----------

# MAGIC %md ### 1. group multiple rows in single using pyspark

# COMMAND ----------

data=[(1,'Manish','Mobile'),(1,'Manish','Washing Mavhine'),(2,'Rahul','Car'),(2,'Rahul','mobile'),(2,'Rahul','scooty'),(3,'Monu','Scooty')]
schema=["Customer_ID", "Customer_Name",'Purchase']

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df.createTempView('sample')

# COMMAND ----------

# MAGIC %sql
# MAGIC select Customer_ID,Customer_Name, collect_set(Purchase) as Purchase from sample group by 1,2

# COMMAND ----------

from pyspark.sql.functions import *
df.groupBy('Customer_ID','Customer_Name').agg(collect_set("Purchase").alias("Purchase")).show(truncate=False)

# COMMAND ----------

# MAGIC %md ### combine list and convert into dataframe using pyspark

# COMMAND ----------

list1 = ["a", "b", "c", "d"]
list2 = [1, 2, 3, 4]

data = [{"col1": a, "col2": b} for a, b in zip(list1, list2)]
df = spark.createDataFrame(data)
df.show()


# COMMAND ----------

# MAGIC %md ### Faltten data

# COMMAND ----------

data = [
  ("Manish",["Java","Scala","C++"]),
  ("rahul",["Spark","Java","C++","Spark","Java"]),
  ("shyam",["CSharp","VB","Spark","Python"])
]
columns=["name","language"]

df = spark.createDataFrame(data,columns)
df.show(truncate=False)


# COMMAND ----------

new_df = df.select('name', explode("language").alias("language"))
new_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md ### find first not null values

# COMMAND ----------

data=[('Goa', '', 'AP'),('', 'AP', None), (None, '', 'Bglr')]
columns=["city1", "city2", "city3"]
df1=spark.createDataFrame(data, columns)
display(df1)

# COMMAND ----------

from pyspark.sql.functions import *
df1.withColumn('output',coalesce(
    when(df1.city1=='', None).otherwise(df1.city1),
    when(df1.city2=='', None).otherwise(df1.city2),
    when(df1.city3=='', None).otherwise(df1.city3)
)).show()

# COMMAND ----------

# MAGIC %md ### PySpark interview questions
# MAGIC How to handle null values in pyspark 
# MAGIC pyspark null values problem

# COMMAND ----------

data = [
    (101, "Alice", "HR", 5000.0, "2021-01-15", None),
    (102, "Bob", None, None, "2021-03-20", "New York"),
    (103, "Charlie", "IT", 7000.0, None, "Los Angeles"),
    (104, None, "Finance", None, "2021-07-10", None),
    (105, "Eve", None, 4500.0, "2021-09-01", "Chicago"),
    (106, None, "HR", None, None, None)
]
schema=['id','name','dept','salary','date','country']

df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import *
df.na.fill(0).show()

df.na.fill('name', 'Unknown').show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import functions as F
df.select([count(when(col(i).isNull(), i)).alias(i) for i in df.columns]).show()

# COMMAND ----------

display(
  df.na.fill({'name': 'Unknown'})
)

# COMMAND ----------

mean_salary = df.select(mean(df.salary)).collect()[0][0]
print(mean_salary)

# COMMAND ----------

# MAGIC %md ### Find customers who have placed orders on consecutive days

# COMMAND ----------

data = [
    (1, '2024-10-01'),
    (1, '2024-10-02'),
    (1, '2024-10-04'),
    (2, '2024-10-03'),
    (2, '2024-10-05'),
    (3, '2024-10-01'),
    (3, '2024-10-02'),
    (3, '2024-10-03'),
]

df = spark.createDataFrame(data, ["customer_id", "order_date"])
df.display()

# COMMAND ----------

from pyspark.sql.functions import lag, datediff, col
from pyspark.sql.window import Window

windowspec = Window.partitionBy("customer_id").orderBy("order_date")

df_lag = df.withColumn(
    "previous_days",
    lag("order_date").over(windowspec)
)

df_consecutive_days = df_lag.withColumn(
    "days_diff",
    datediff(
        col("order_date"),
        col("previous_days")
    )
)

display(df_consecutive_days)

# df_lead = df.withColumn("next_days", lead("order_date").over(windowspec)).show()

# COMMAND ----------

filtered_df = df_consecutive_days.filter(df_consecutive_days.days_diff == 1)
display(filtered_df)

# COMMAND ----------

df_consecutive_days.select("customer_id").distinct().show()

filtered_df.select("customer_id").distinct().show()

# COMMAND ----------

# MAGIC %md ### Employee salary greater than his manager salary 
# MAGIC %md ### Solve interview question using pyspark 
# MAGIC %md ### solver interview question using sql
# MAGIC

# COMMAND ----------

schema = ["employee_id","employee_name","manager_id","salary"]

#Create data

data = [
    (1, "Alice", 3, 5000),   # Alice's manager is Charlie
    (2, "Bob", 3, 4000),     # Bob's manager is Charlie
    (3, "Charlie", 5, 12000), # Charlie is managed by Eve
    (4, "David", 5, 3000),   # David's manager is Eve
    (5, "Eve", None, 10000)  # Eve is the top-level manager
]

df=spark.createDataFrame(data,schema)
df.show()
display(df)

# COMMAND ----------

df.createTempView("sample")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from sample s
# MAGIC join sample m
# MAGIC on m.manager_id = s.employee_id
# MAGIC where s.salary > m.salary
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

schema = ["UserID", "UserName", "Login", "Logout"]

#Sample data for users with multiple login/logout sessions
data = [
    ("U001", "Alice Johnson", "2024-12-15 08:30:00", "2024-12-15 09:30:00"),
    ("U001", "Alice Johnson", "2024-12-15 10:00:00", "2024-12-15 11:00:00"),
    ("U002", "Bob Smith", "2024-12-15 09:00:00", "2024-12-15 09:45:00"),
    ("U002", "Bob Smith", "2024-12-15 10:30:00", "2024-12-15 12:00:00"),
    ("U002", "Bob Smith", "2024-12-15 13:00:00", "2024-12-15 14:00:00"),
    ("U003", "Charlie Brown", "2024-12-15 08:45:00", None),
    ("U003", "Charlie Brown", "2024-12-15 14:00:00", "2024-12-15 15:30:00"),
    ("U004", "Diana Prince", "2024-12-15 10:00:00", "2024-12-15 11:30:00"),
    ("U004", "Diana Prince", "2024-12-15 12:00:00", None)
]

df=spark.createDataFrame(data,schema)
df.show()
df.printSchema()

# COMMAND ----------

# convert string to new timestamp
df = df.withColumn("Login", df["Login"].cast(TimestampType()))
df = df.withColumn("Logout", df["Logout"].cast(TimestampType()))
df.show()
df.printSchema()


# COMMAND ----------

df.createTempView("userdetails")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from userdetails

# COMMAND ----------

# MAGIC %sql
# MAGIC select UserID,
# MAGIC count(*) total_user_count,
# MAGIC min(Login) as first_login,
# MAGIC max(Login) as last_login,
# MAGIC max(Logout) as last_logout,
# MAGIC min(Logout) as first_logout,
# MAGIC sum(case when Login is not null then 1 else 0 end) as total_login_count,
# MAGIC sum(case when Logout is not null then 1 else 0 end) as total_logout_count
# MAGIC from userdetails
# MAGIC group by UserID

# COMMAND ----------

# MAGIC %md ### last login time

# COMMAND ----------

from pyspark.sql.functions import *
df.select(max("Login").alias("last login")).show()

# COMMAND ----------

# MAGIC %md ### last logout time

# COMMAND ----------

from pyspark.sql.functions import *
df.select(max("Logout").alias("last logout")).show()

# COMMAND ----------

# MAGIC %md ### first login time

# COMMAND ----------

from pyspark.sql.functions import *
df.select(min("Login").alias("first login")).show()

# COMMAND ----------

from pyspark.sql.functions import *
df.select(min("Logout").alias("last logout")).show()

# COMMAND ----------

# MAGIC %md ### total login count

# COMMAND ----------

from pyspark.sql.functions import *
df.filter(col("Login").isNotNull()).count()



# COMMAND ----------

# MAGIC %md ### total logout count

# COMMAND ----------

from pyspark.sql.functions import *
df.filter(col("Logout").isNotNull()).count()

# COMMAND ----------

# MAGIC %md ### total duration per user

# COMMAND ----------

from pyspark.sql.functions import *

df.groupBy("UserID").agg(count("Login").alias("total_login_count")).show()

# COMMAND ----------

from pyspark.sql.functions import *

df.groupBy("UserID").agg(count("Logout").alias("total_logout_count")).show()

# COMMAND ----------

df = spark.read.json("/Volumes/databricksansh/bronze/json_voulme/order_singleline.json")
df.show(truncate=False)
df.printSchema()

# COMMAND ----------

df = spark.read.option(
    "multiline", "true"
).format(
    "json"
).load(
    "/Volumes/databricksansh/bronze/json_voulme/order_multiline.json"
)
display(df)

# COMMAND ----------

df_1 = spark.range(4, 200, 2)
df_2 = spark.range(2, 200, 4)

# COMMAND ----------

df_3 = df_1.repartition(5)
df_4 = df_2.repartition(7)

# COMMAND ----------

df_joined = df_3.join(df_4, on="id")

# COMMAND ----------

df_sum = df_joined.selectExpr("sum(id) as total_sum")

# COMMAND ----------

df_sum.show()

# COMMAND ----------

df_sum.explain()

# COMMAND ----------

!python --version

# COMMAND ----------

# MAGIC %sh python --version

# COMMAND ----------

# MAGIC %pip list

# COMMAND ----------

# MAGIC %pip show pandas

# COMMAND ----------

import random

# Create a sample DataFrame 'df1' with 100,000 rows
data1 = [(i, f'Name_{i}', random.randint(60, 100)) for i in range(1, 100001)]
df1 = spark.createDataFrame(data1, ["ID", "Name", "Score1"])

# Create another sample DataFrame 'df2' with 100,000 rows
data2 = [(random.randint(1, 100000), random.randint(60, 100), f'City_{i}') for i in range(1, 100001)]
df2 = spark.createDataFrame(data2, ["ID", "Score2", "City"])

# COMMAND ----------

df1.show()

df2.show()

# COMMAND ----------

# MAGIC %fs ls dbfs:/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/

# COMMAND ----------

df = spark.read.csv("/Volumes/databricksansh/bronze/emp_volume/emp.csv", header=True)

df.show()

df.dtypes

# COMMAND ----------

df.columns

# COMMAND ----------


df.schema

# COMMAND ----------

df.describe()

# COMMAND ----------

df.describe().display()

# COMMAND ----------

# MAGIC %md ### Surrogate Key Generation
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create table dev_env.bronze.test01 (
# MAGIC   SurrogateKey int comment 'surrogate key',
# MAGIC   ID int comment 'id',
# MAGIC   FirstName string comment 'first name',
# MAGIC   LastName string comment 'last name'
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_env.bronze.test01

# COMMAND ----------

test_nrows = 1000000
test_npartitions = 40

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dev_env.bronze.test01 (ID, FirstName, LastName)
# MAGIC values (1, 'sonal', 'holankar');
# MAGIC
# MAGIC insert into dev_env.bronze.test01 (ID, FirstName, LastName)
# MAGIC values (2, 'ram', 'patil');
# MAGIC
# MAGIC insert into dev_env.bronze.test01 (ID, FirstName, LastName)
# MAGIC values (3, 'rama', 'jadhav');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_env.bronze.test01

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

df_with_sk = df.withColumn(
    "surrogate_key",
    monotonically_increasing_id()
)

df_with_sk.show()


# COMMAND ----------

display(df)

# COMMAND ----------

df.createTempView('emp')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE demo (
# MAGIC   id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   product_type STRING,
# MAGIC   sales BIGINT
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO demo (product_type, sales)
# MAGIC VALUES ("Batteries", 150000);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from workspace.default.demo

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

data = [
    ('alex', 'mumbai'),
    ('bob', 'pune'),
    ('john', 'nashik')
]

df = spark.createDataFrame(
    data,
    ['name', 'city']
)
display(df)


# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

df1 = df.withColumn("surrogate_key", monotonically_increasing_id())
df1.show()


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

df2 = df.withColumn(
    "surrogate_key",
    row_number().over(Window.orderBy("name"))
)

df2.show()


# COMMAND ----------

from pyspark.sql.functions import expr

df3 = df.withColumn("surrogate_key", expr("uuid()"))
df3.show(truncate=False)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE employee (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   dept STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended employee

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE employee_ext (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   dept STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from system.information_schema.external_locations
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sales (
# MAGIC   order_id INT,
# MAGIC   order_date DATE,
# MAGIC   country STRING,
# MAGIC   amount DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (country);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sales VALUES
# MAGIC (1,'2024-01-01','US',100),
# MAGIC (2,'2024-01-02','IN',200);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales WHERE country = 'US';
# MAGIC

# COMMAND ----------

df = spark.table('sales')

# COMMAND ----------

df.write \
  .partitionBy("country") \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("sales")


# COMMAND ----------

df.repartition(4)

# COMMAND ----------

df.coalesce(1).write.mode("overwrite").format("delta").saveAsTable("sales_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended workspace.default.sales_new

# COMMAND ----------

# MAGIC %md Create DF from List

# COMMAND ----------

from pyspark.sql import Row

data = [
    Row(id=1, name="Alex", age=25),
    Row(id=2, name="John", age=30),
    Row(id=3, name="Sara", age=28)
]

df = spark.createDataFrame(data)
df.show()
df.printSchema()


# COMMAND ----------

# MAGIC %md ### cume_dist() ( cumulative distribution ) is a window function in Spark SQL used to find the relative position of a row within a partition, based on ordering.

# COMMAND ----------

from pyspark.sql import Row

data = [
    Row(name='a', score=50),
    Row(name='b', score=60),
    Row(name='c', score=80),
    Row(name='d', score=95)
]

df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import cume_dist

window_spec = Window.orderBy("score")

df_with_cd = df.withColumn(
    "cume_dist",
    cume_dist().over(window_spec)
)

df_with_cd.show()


# COMMAND ----------

display(dbutils.fs.ls("dbfs:/"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets/airlines/"))

# COMMAND ----------

display(dbutils.fs.head("dbfs:/databricks-datasets/airlines/part-00000"))

# COMMAND ----------

ar_df = spark.read.csv("dbfs:/databricks-datasets/airlines/part-00000", header=True, inferSchema=True)
ar_df.printSchema()
ar_df.show(5)

# COMMAND ----------

ar_df.cache()

# COMMAND ----------

# MAGIC %md ### Json Support in Spark

# COMMAND ----------

df = spark.read.json("dbfs:/databricks-datasets/")
df.show(5)
df.printSchema()



# COMMAND ----------

df = spark.read.option("header", True).csv("dbfs:/databricks-datasets/COVID/coronavirusdataset/SeoulFloating.csv")

# COMMAND ----------

df.toPandas().count()

# COMMAND ----------

import pyspark.pandas as ps

data = {
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "score": [85, 90, 88]
}

psdf = ps.DataFrame(data)
psdf


# COMMAND ----------

import pyspark.pandas as ps

psdf = ps.read_csv("dbfs:/databricks-datasets/airlines/part-00000")

# Convert to Spark DataFrame
sdf = psdf.to_spark()
sdf.createOrReplaceTempView("airlines")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_version();
# MAGIC

# COMMAND ----------

spark.version

# COMMAND ----------

# sql version
display(
    spark.sql(
        "SELECT version()"
    )
)


# COMMAND ----------

import sys
sys.version


# COMMAND ----------

import sys
sys.version_info


# COMMAND ----------

import platform
platform.python_version()

# COMMAND ----------

dbutils.widgets.multiselect(
    "countries",
    "US",
    ["US", "IN", "UK", "CA"]
)


# COMMAND ----------

dbutils.widgets.text("input_path", "dbfs:/databricks-datasets")
path = dbutils.widgets.get("input_path")

df = spark.read.json(path)
df.show()


# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

from delta.tables import DeltaTable

help(DeltaTable.vacuum)

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets/"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets/samples/lending_club/"))

# COMMAND ----------

# MAGIC %md ### Schema evolution - MergeSchema option

# COMMAND ----------

# MAGIC %sql
# MAGIC create table dev_env.bronze.loan_new
# MAGIC using delta
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO dev_env.bronze.loan_new
# MAGIC FROM 'dbfs:/databricks-datasets/samples/lending_club/parquet/'
# MAGIC FILEFORMAT = PARQUET
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_env.bronze.loan_new

# COMMAND ----------


df = spark.read.table("dev_env.bronze.loan_new")


# COMMAND ----------

from pyspark.sql.functions import current_timestamp
new_df = df.withColumn('processed_date', current_timestamp())
display(new_df)

# COMMAND ----------

new_df.write.format("delta").mode("overwrite").saveAsTable(
    "dev_env.silver.loan"
)

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets/learning-spark-v2/"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets/adult/"))

# COMMAND ----------

df = spark.read.format("csv").load("dbfs:/databricks-datasets/adult/adult.data", header=True)
display(df)

# COMMAND ----------

df_clean = df.toDF(
    *[
        col.replace(" ", "_")
        .replace("-", "_")
        .replace(",", "_")
        .replace(";", "_")
        .replace("{", "_")
        .replace("}", "_")
        .replace("(", "_")
        .replace(")", "_")
        .replace("\n", "_")
        .replace("\t", "_")
        .replace("=", "_")
        for col in df.columns
    ]
)
df_clean.repartition(1).write.format("delta").mode("overwrite").saveAsTable("dev_env.silver.adult")

# COMMAND ----------

df_clean.repartition(1).write.format("json").mode("overwrite").save("/Volumes/databricksansh/bronze/landing")


# COMMAND ----------

df = spark.read.json(
    "/Volumes/databricksansh/bronze/landing/part-00000-tid-490004710391847276-f89d4c1e-0b1d-413a-828c-1af931773cc2-293-1-c000.json"
)
display(df)

# COMMAND ----------

from pyspark.sql.functions import lit
ex_df = df.withColumn("Null_Col", lit(None))

# COMMAND ----------

from pyspark.sql.functions import lit
ex3_df = ex_df.withColumn("Null_Col", lit('None_varchar'))
display(ex3_df)

# COMMAND ----------

display(ex_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dev_env.bronze.eps01
# MAGIC (
# MAGIC   date STRING,
# MAGIC   stock_symbol STRING,
# MAGIC   analyst STRING,
# MAGIC   eps DOUBLE
# MAGIC )
# MAGIC USING DELTA ;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE dev_env.bronze.eps01
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended dev_env.bronze.eps01

# COMMAND ----------

# MAGIC %md ### Add Image in Databricks

# COMMAND ----------

df1 = spark.read.format("binaryFile").load(
"/Volumes/dev_env/bronze/img_volume/1.jpg"
)
display(df1)


# COMMAND ----------

df2 = spark.read.format("binaryFile").load("/Volumes/dev_env/bronze/img_volume/2.jpg")
display(df2)


# COMMAND ----------

from PIL import Image
import matplotlib.pyplot as plt

img = Image.open("/Volumes/dev_env/bronze/img_volume/1.jpg")
plt.imshow(img)
plt.axis("off")


# COMMAND ----------

from PIL import Image
import matplotlib.pyplot as plt

img = Image.open("/Volumes/dev_env/bronze/img_volume/2.jpg")
plt.imshow(img)
plt.axis("off")


# COMMAND ----------

dbutils.secrets.list("jdbc")


# COMMAND ----------

dbutils.secrets.list("uc_prod_scope")

# COMMAND ----------

# MAGIC %md ### Add prefix to all column names in spark data frame Databricks

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets/"))

# COMMAND ----------

df = spark.read.csv(
    "dbfs:/databricks-datasets/nyctaxi/tripdata/yellow",
    header=True
)

display(df)

# COMMAND ----------

df.columns

# COMMAND ----------

from pyspark.sql.functions import col
sl = [col(col_name).alias("my_prefix_" + col_name) for col_name in df.columns]

# COMMAND ----------

df.select(*sl).show()
df=df.select(*sl)

# COMMAND ----------

# MAGIC %md ### Get Cluster Id of all cluster in Data bricks using API

# COMMAND ----------

import requests

url = "https://dbc-9f8d1b0f-90f4.cloud.databricks.com/api/2.0/clusters/list"
headers = {
    "Authorization": "Bearer dapidbe2f250d206f7cd7c4ef3d122c7d607"
}

response = requests.get(
    url,
    headers=headers,
    verify=True
)

print(response.json())  

# COMMAND ----------

# MAGIC %md ### Get workspace Host Name in Data bricks

# COMMAND ----------

api_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().toString()

# COMMAND ----------

print(api_url)

# COMMAND ----------

# MAGIC %md ### Read Excel File in Datatbricks

# COMMAND ----------

# MAGIC %pip install openpyxl
# MAGIC
# MAGIC import pandas as pd
# MAGIC
# MAGIC pdf = pd.read_excel("/Volumes/databricksansh/bronze/excel_volume/finance_sample.xlsx", sheet_name="Sheet1")
# MAGIC df = spark.createDataFrame(pdf)
# MAGIC
# MAGIC display(df)
# MAGIC
# MAGIC

# COMMAND ----------

print("hello")

# COMMAND ----------

# MAGIC %md ### Logging in Databricks

# COMMAND ----------

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("my_databricks_logger")

logger.info("Notebook started")
logger.warning("This is a warning")
logger.error("Something went wrong")


# COMMAND ----------

pip list

# COMMAND ----------

# MAGIC %pip install dbdemos

# COMMAND ----------

import dbdemos
dbdemos.install("lakehouse-intro")

# COMMAND ----------

dbdemos.list_demos()

# COMMAND ----------

dbdemos.install('lakehouse-fsi-credit')

# COMMAND ----------

dbdemos.install('cdc-pipeline')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe volume databricksansh.bronze.bronze_volume

# COMMAND ----------

# MAGIC %md ### Variant Data Type

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE databricksansh.bronze.events (
# MAGIC   raw VARIANT
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO databricksansh.bronze.events
# MAGIC VALUES (parse_json('{"user_id":123,"action":"click"}'));
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC *
# MAGIC FROM databricksansh.bronze.events;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   raw:user_id,
# MAGIC   raw:action
# MAGIC FROM databricksansh.bronze.events;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE databricksansh.bronze.events_new (
# MAGIC   event_id STRING,
# MAGIC   payload VARIANT
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO databricksansh.bronze.events_new VALUES (
# MAGIC   'e1',
# MAGIC   parse_json('{
# MAGIC     "user": { "id": 123, "name": "Alice" },
# MAGIC     "action": "click",
# MAGIC     "success": true
# MAGIC   }')
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricksansh.bronze.events_new

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from system.billing.usage

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from system.billing.account_prices

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from system.billing.list_prices

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   payload:user.id AS user_id,
# MAGIC   payload:action AS action
# MAGIC FROM databricksansh.bronze.events_new ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- access array 
# MAGIC SELECT
# MAGIC   payload:items[0].price::DOUBLE AS first_price
# MAGIC FROM databricksansh.bronze.events_new;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   payload:items[0]
# MAGIC FROM databricksansh.bronze.events_new;

# COMMAND ----------

# MAGIC %sql
# MAGIC with list_cost_per_job as (
# MAGIC   SELECT
# MAGIC     t1.workspace_id,
# MAGIC     t1.usage_metadata.job_id,
# MAGIC     COUNT(DISTINCT t1.usage_metadata.job_run_id) as runs,
# MAGIC     SUM(t1.usage_quantity * list_prices.pricing.default) as list_cost,
# MAGIC     first(identity_metadata.run_as, true) as run_as,
# MAGIC     first(t1.custom_tags, true) as custom_tags,
# MAGIC     MAX(t1.usage_end_time) as last_seen_date
# MAGIC   FROM system.billing.usage t1
# MAGIC   INNER JOIN system.billing.list_prices list_prices on
# MAGIC     t1.cloud = list_prices.cloud and
# MAGIC     t1.sku_name = list_prices.sku_name and
# MAGIC     t1.usage_start_time >= list_prices.price_start_time and
# MAGIC     (t1.usage_end_time <= list_prices.price_end_time or list_prices.price_end_time is null)
# MAGIC   WHERE
# MAGIC     t1.billing_origin_product = "JOBS"
# MAGIC     AND t1.usage_date >= CURRENT_DATE() - INTERVAL 30 DAY
# MAGIC   GROUP BY ALL
# MAGIC ),
# MAGIC most_recent_jobs as (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER(PARTITION BY workspace_id, job_id ORDER BY change_time DESC) as rn
# MAGIC   FROM
# MAGIC     system.lakeflow.jobs QUALIFY rn=1
# MAGIC )
# MAGIC SELECT
# MAGIC     t2.name,
# MAGIC     t1.job_id,
# MAGIC     t1.workspace_id,
# MAGIC     t1.runs,
# MAGIC     t1.run_as,
# MAGIC     SUM(list_cost) as list_cost,
# MAGIC     t1.last_seen_date
# MAGIC FROM list_cost_per_job t1
# MAGIC   LEFT JOIN most_recent_jobs t2 USING (workspace_id, job_id)
# MAGIC GROUP BY ALL
# MAGIC ORDER BY list_cost DESC