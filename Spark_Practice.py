# Databricks notebook source
df = spark.read.csv ("/Volumes/databricksansh/bronze/jobvolume/emp_new.csv", header=True , inferSchema=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.groupBy("address").sum("salary").show()

# COMMAND ----------

df.groupBy("address").max("salary").show()

# COMMAND ----------

df.groupBy("address").min("salary").show()

# COMMAND ----------

df.groupBy("address").count().show()

# COMMAND ----------

df.groupBy("address","emp_id").sum("salary").show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Join Types

# COMMAND ----------

df2 = spark.read.csv ("/Volumes/databricksansh/bronze/jobvolume/raw/department.csv", header=True , inferSchema=True)

# COMMAND ----------

df2.show()

# COMMAND ----------

df2.withColumnRenamed("user" ,"emp_id")

# COMMAND ----------

df2.show()

# COMMAND ----------

emp_df = spark.read.csv("/Volumes/databricksansh/bronze/jobvolume/raw/emp_2.csv", header=True , inferSchema=True)
emp_df.show()


# COMMAND ----------

dept_df = spark.read.csv("/Volumes/databricksansh/bronze/jobvolume/department_1.csv", header=True , inferSchema=True)
dept_df.show()


# COMMAND ----------

emp_df.join(dept_df, emp_df.emp_id == dept_df.user , 'inner').show()

# COMMAND ----------

emp_df.join(dept_df, emp_df.emp_id == dept_df.user , 'left').show()

# COMMAND ----------

emp_df.join(dept_df, emp_df.emp_id == dept_df.user , 'right').show()

# COMMAND ----------

emp_df.join(dept_df, emp_df.emp_id == dept_df.user , 'fullouter').show()

# COMMAND ----------

emp_df.join(dept_df, emp_df.emp_id == dept_df.user , 'left_anti').show()

# COMMAND ----------

emp_df.join(dept_df, emp_df.emp_id == dept_df.user , 'left_semi').show()

# COMMAND ----------

from pyspark.sql.functions import *
data1 = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000) \
  ]

columns= ["employee_name","department","state","salary","age","bonus"]

# COMMAND ----------

first_df = spark.createDataFrame(data=data1, schema=columns)
first_df.show()


# COMMAND ----------

data2 = [("James","Sales","NY",90000,34,10000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]
columns2= ["employee_name","department","state","salary","age","bonus"]

# COMMAND ----------

second_df = spark.createDataFrame(data=data2, schema=columns2)
second_df.show()

# COMMAND ----------

first_df.union(second_df).show()

# COMMAND ----------

first_df.unionAll(second_df).show()

# COMMAND ----------

# for unique records use distinct

first_df.union(second_df).distinct().show()

# COMMAND ----------

df = spark.read.csv("/Volumes/databricksansh/bronze/jobvolume/sample.csv",header=True, inferSchema=True)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show()

# COMMAND ----------

df.na.fill("").show()

# COMMAND ----------

df.na.fill("unknown").show()

# COMMAND ----------

df.fillna("").show()

# COMMAND ----------

df.fillna("unknown").show()

# COMMAND ----------

df.show()

# COMMAND ----------

# specific column filling

df.na.fill("","city").show()

# COMMAND ----------

df.fillna("").show()

# COMMAND ----------

# collect method
df.collect()


# COMMAND ----------

data = [(1,'manish','usa'),(2,'mani','usa'),(1,'nish','usa')]
columns = ['id','name','city']
df = spark.createDataFrame(data, columns)
df.show()
df.printSchema()

# COMMAND ----------

# pivot --- rows to columns

# unpivot -- columns to rows

# COMMAND ----------

data = [("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"), \
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"), \
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"), \
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]

columns= ["Product","Amount","Country"]

df = spark.createDataFrame(data,columns)
df.show()


# COMMAND ----------

df.groupBy("Product").pivot("Country").sum("Amount").show()

# COMMAND ----------

df1 = df.groupBy("Product").pivot("Country").sum("Amount").show()

# COMMAND ----------

# UDF --- User Defined Function

# COMMAND ----------

data = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
Columns = ["dept_name","dept_id"]

df = spark.createDataFrame(data,Columns)
df.show()

# COMMAND ----------

from pyspark.sql.types import LongType
def addone(a):
    return a+1

addone_df=udf(addone, LongType())

# COMMAND ----------

df.select("dept_name","dept_id",addone_df(df.dept_id).alias("new_dept_id")).show()


# COMMAND ----------

# Transform() Function for custom transformations and return new df after transformation


# COMMAND ----------

data=[(1,'manish',10000),(2,'rani',50000),(3,'sunny',5000)]
columns=['id','name','salary']

df =spark.createDataFrame(data,columns)
df.show()

# COMMAND ----------

from pyspark.sql.functions import upper
def uppername(df):
    return df.withColumn("name",upper(df.name))

df.transform(uppername).show()    

# COMMAND ----------

df.show()

# COMMAND ----------

df.createOrReplaceTempView("employee")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee

# COMMAND ----------

# MAGIC %md
# MAGIC # Windows Functions in Spark

# COMMAND ----------

data=[(1,'manish','india',10000),(2,'rani','india',50000),(3,'sunny','UK',5000),(4,'sohan','UK',25000),(5,'mona','india',10000)]
columns=['id','name','country','salary']

df =spark.createDataFrame(data,columns)
df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
window = Window.partitionBy("country").orderBy("salary")
df.withColumn("rn",row_number().over(window)).show()


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank
window = Window.partitionBy("country").orderBy("salary")
df.withColumn("rank",rank().over(window)).show()


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank
window = Window.partitionBy("country").orderBy("salary")
df.withColumn("rank",dense_rank().over(window)).show()


# COMMAND ----------

# MAGIC %md
# MAGIC **Date **Functions****

# COMMAND ----------

data = [("2022-03-15", "2022-03-16 12:34:56.789"), 
        ("2022-03-01", "2022-03-16 01:23:45.678")]
df = spark.createDataFrame(data, ["date_col", "timestamp_col"])
df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import *

df.select("date_col",date_add("date_col",30)).show()

# COMMAND ----------

from pyspark.sql.functions import date_sub, datediff

df2 = df.select("date_col",date_sub("date_col", 7).alias("new_date"))
df2.show()
df2.select("date_col","new_date",datediff("date_col", "new_date").alias("diff_days")).show()


# COMMAND ----------

df.select("date_col",year("date_col").alias("Year")).show()

# COMMAND ----------

df.select("date_col",month("date_col").alias("Month")).show()
df.select("date_col",dayofmonth("date_col").alias("Day")).show()

# COMMAND ----------

df.select("date_col",dayofweek("date_col").alias("Weekday")).show()

# COMMAND ----------

df.select("date_col",dayofyear("date_col").alias("Day in Year")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Partition Practice

# COMMAND ----------

# explode function

data = [
  ("Manish",["Java","Scala","C++"]),
  ("rahul",["Spark","Java","C++","Spark","Java"]),
  ("shyam",["CSharp","VB","Spark","Python"])
]
columns=["name","language"]

df = spark.createDataFrame(data, columns)

df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import explode

df.select("name",explode("language")).show()

# COMMAND ----------

df.createOrReplaceTempView("sample")

# COMMAND ----------

# MAGIC %sql select * from sample

# COMMAND ----------

# MAGIC %sql 
# MAGIC select name ,explode(language) from sample

# COMMAND ----------

# MAGIC %md
# MAGIC # Print duplicate and remove duplicates

# COMMAND ----------

data =[(1,'abc@gmail.com'),(2,'def@gmail.com'),(1,'abc@gmail.com')]
column=['id','name']

df =spark.createDataFrame(data,column)
df.show()

# COMMAND ----------

from pyspark.sql.functions import *
display(df.groupBy("name").count().filter(col('count')>1))

# COMMAND ----------

df.createOrReplaceTempView("sample")

# COMMAND ----------

# MAGIC %sql select * from sample

# COMMAND ----------

# MAGIC %sql select count(*),name from sample group by name having count(*)>1

# COMMAND ----------

# MAGIC %sql select distinct name, id from sample;

# COMMAND ----------

df.distinct().show()

# COMMAND ----------

# MAGIC %md Interview Questions
# MAGIC
# MAGIC Customers who ordered
# MAGIC Customers who never ordered

# COMMAND ----------

customer_data=[(1,'Manish'),(2,'Rahul'),(3,'Monu'),(4,'Ram')]
schema=["Customer_ID", "Customer_Name"]

customer_df = spark.createDataFrame(customer_data,schema)
customer_df.show()


# COMMAND ----------

order_data=[(1,4),(3,2)]
schema1=["Order_ID", "Customer_ID"]

order_df = spark.createDataFrame(order_data,schema1)
order_df.show()

# COMMAND ----------

display(customer_df.join(order_df, customer_df.Customer_ID == order_df.Customer_ID, "left").filter(order_df.Order_ID.isNull()))

# COMMAND ----------

display(customer_df.join(order_df, customer_df.Customer_ID == order_df.Customer_ID, "inner"))

# COMMAND ----------

customer_df.createOrReplaceTempView("customer")
order_df.createOrReplaceTempView("order")

# COMMAND ----------

# MAGIC %sql
# MAGIC select c.Customer_ID
# MAGIC from customer c
# MAGIC left join order o
# MAGIC on c.Customer_ID = o.Customer_ID
# MAGIC where o.Order_ID is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select c.Customer_ID
# MAGIC from customer c
# MAGIC inner join order o
# MAGIC on c.Customer_ID = o.Customer_ID

# COMMAND ----------

# MAGIC %md
# MAGIC PYSPARK INTERVIEW QUESTION 
# MAGIC WE HAVE 2 TABLE EMPLOYEE AND DEPARTMENT TABLE IS GIVEN 
# MAGIC
# MAGIC 1. WE HAVE TO FIND THE HIGHEST SALARY BASED ON EACH DEPARTMENT NAME
# MAGIC 2.  WE HAVE TO FIND THE EMPLOYEE WHO IS GETTING HIGHEST SALARY BASED ON EACH DEPARTMENT NAME
# MAGIC 3. WE HAVE TO FIND THE lowest SALARY BASED ON EACH DEPARTMENT NAME
# MAGIC 4.  WE HAVE TO FIND THE EMPLOYEE WHO IS GETTING lowest SALARY BASED ON EACH DEPARTMENT NAME

# COMMAND ----------

emp_data=[('Manish' , 1 , 75000),
('Raghav' , 1 , 85000 ),
('surya' , 1 , 80000 ),
('virat' , 2 , 70000),
('rohit' , 2 , 75000),
('jadeja' , 3 , 85000),
('anil' , 3 , 55000),
('sachin' , 3 , 55000),  
('zahir', 4, 60000),
('bumrah' , 4 , 65000) ]
schema= ["emp_name" ,"dept_id" ,"salary"]

df1 = spark.createDataFrame(emp_data,schema)
df1.show()

dept_data = [(1, 'DATA ENGINEER'),(2, 'SALES'),(3, 'SOFTWARE'),(4, 'HR')]
schema1=['dept_id','dept_name']

df2 = spark.createDataFrame(dept_data,schema1)
df2.show()

# COMMAND ----------

from pyspark.sql.functions import max
df = df1.join(df2, df1.dept_id == df2.dept_id, "left")
# df.show()
df3 = df.groupBy('dept_name').agg(max('salary').alias("Highest Salary"))
df3.show()

# COMMAND ----------

from pyspark.sql.functions import *
low_df = df.groupBy('dept_name').agg(min('salary').alias("Lowest Salary"))
low_df.show()

# COMMAND ----------



# COMMAND ----------

df3.show()

# COMMAND ----------

df.show()

# COMMAND ----------

df4 = df.join(df3, df.dept_name == df3.dept_name, "left")
df4.show()

# COMMAND ----------

from pyspark.sql.functions import col
df4.filter(col('salary')== col('Highest Salary')).show()

# COMMAND ----------

from pyspark.sql.functions import *
data = [(1,['mobile','PC','Tab']),(2,['mobile','PC']),(3,['Tab','Pen'])]
schema=['customer_id','product_purchase']

df = spark.createDataFrame(data,schema)
df.withColumn('product',explode('product_purchase')).select('customer_id','product').show()
# df.show()


# COMMAND ----------

# find first not null value


from pyspark.sql.functions import *
data=[(1, 'yes',None,None),(2, None,'yes',None),(3, 'No',None,'yes')]
schema=['customer_id','device_using1','device_using2','device_using3']

df = spark.createDataFrame(data , schema)
df.withColumn('new',coalesce(col("device_using1"),col("device_using2"),col("device_using3"))).show()

# COMMAND ----------

# MAGIC %md  Pyspark interview question
# MAGIC 1. WE are getting data in the form of string JSON we need to convert into json format using pyspark
# MAGIC 2. After converting we need to create seperate column from the json body.
# MAGIC using pyspark 

# COMMAND ----------

data=[('Manish','{"street": "123 St", "city": "Delhi"}'),('Ram','{"street": "456 St", "city": "Mumbai"}')]
schema=['name','address']
df=spark.createDataFrame(data,schema)
print(df.printSchema())
display(df)

# COMMAND ----------

# using sql

df.createOrReplaceTempView('people')

# COMMAND ----------

# MAGIC %sql  select * from people

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(
# MAGIC select name, address,from_json(address, 'street string, city string') as address_new from people
# MAGIC )
# MAGIC
# MAGIC select name , address_new, address_new.street as street , address_new.city  as city from cte

# COMMAND ----------

from pyspark.sql.functions import *

df1 = df.withColumn('address_new',from_json(col('address'),'street string, city string'))

display(df1)

# COMMAND ----------


df1.select("name","address",col("address_new").street.alias("street"),col("address_new").city.alias("city")).show()

# COMMAND ----------

# MAGIC %md Cummulative sales - PySpark Interview Question
# MAGIC Cummulative sales  Interview Question using pyspark
# MAGIC Cummulative sales  Interview Question using spark sql
# MAGIC

# COMMAND ----------

data = [ ['2024-01-01',20000], ['2024-01-02',10000],[ '2024-01-03',150000], ['2024-01-04',100000], ['2024-01-05',210000]] 
  
#define column names
columns = ['date', 'sales'] 
  
#create dataframe using data and column names
df = spark.createDataFrame(data, columns) 
display(df)

# COMMAND ----------

### limit function in pyspark

# COMMAND ----------

data = [
    (1, "Alice", 5000),
    (2, "Bob", 6000),
    (3, "Charlie", 7000),
    (4, "David", 8000),
    (5, "Eve", 9000),
    (6, "Frank", 10000),
    (7, "Grace", 11000),
    (8, "Hannah", 12000),
    (9, "Ian", 13000),
    (10, "Jack", 14000)
]


# COMMAND ----------

df = spark.createDataFrame(data,["id", "name", "salary"])
df.show()

# COMMAND ----------

df.limit(5).show()

# COMMAND ----------

#limited df
df_limited = df.limit(4)
df_limited.show()

# COMMAND ----------

## describe  function

df.describe().show()

# COMMAND ----------

### difference between first(), head(), and tail() in PySpark

display([df.first()])

display([df.head()])

display([df.tail(2)])

# COMMAND ----------

### exceptAll() Function 

data1 = [("Aamir","IT",5000),
("Shazad","HR",6000),
("Ali","Finance",7000),
("Ram","HR",8000),
("Sham","IT",9000),
("Aamir","IT",5000)]
columns = ["Name","Dept","Salary"]

df1 = spark.createDataFrame(data1, columns)

df1.show()

data2 = [("Aamir","IT",5000),
("Shazad","Finance",5500) ]

columns = ["Name","Dept","Salary"]

df2 = spark.createDataFrame(data2, columns)

df2.show()

# COMMAND ----------

result = df1.exceptAll(df2)
result.show()

# COMMAND ----------

#Intersect() and IntersectAll() in PySpark
# Intersect distinct records in two dataframes
# IntersectAll includes duplicates also

# COMMAND ----------

### exceptAll() Function 

data1 = [("Aamir","IT",5000),
("Shazad","HR",6000),
("Ali","Finance",7000),
("Ram","HR",8000),
("Sham","IT",9000),
("Aamir","IT",5000)]
columns = ["Name","Dept","Salary"]

df1 = spark.createDataFrame(data1, columns)

df1.show()

data2 = [("Aamir","IT",5000),
("Shazad","HR",6000),
("Ali","Finance",7000),
("Zara","Marketing",6000),
("Aamir","IT",5000)]
 
columns = ["Name","Dept","Salary"]

df2 = spark.createDataFrame(data2, columns)

df2.show()

# COMMAND ----------

result1 = df1.intersect(df2)
result1.show()
result2 = df1.intersectAll(df2)
result2.show()

# COMMAND ----------

# na Functions and isEmpty Functions

data1 = [("Aamir","IT",5000),
("Shazad","HR",6000),
("Ali","Finance",7000),
("Ram","HR",8000),
("Sham","IT",9000),
("Aamir","IT",5000),
(None,None,None)]
columns = ["Name","Dept","Salary"]

df1 = spark.createDataFrame(data1, columns)

df1.show()

# COMMAND ----------

df_empty = df1.isEmpty()
print(df_empty)

# COMMAND ----------

df_na_fill = df1.fillna({'Name':'Unknown','Dept':'Unknown','Salary':0})
df_na_fill.show()

# COMMAND ----------

df_na_drop = df1.na.drop()
df_na_drop.show()


# COMMAND ----------

# Cube Functions
data1 = [("Aamir","IT",5000),
("Shazad","HR",6000),
("Ali","Finance",7000),
("Ram","HR",8000),
("Sham","IT",9000)]
columns = ["Name","Dept","Salary"]

df1 = spark.createDataFrame(data1, columns)

df1.show()

# COMMAND ----------

from pyspark.sql.functions import sum
cube_df = df1.cube("Dept","Name").agg(sum("Salary").alias("Total Salary"))
cube_df.orderBy("Dept","Name").show()

# COMMAND ----------

from pyspark.sql.functions import *

agg_df = df1.groupby("Dept").agg(
    sum("Salary").alias("Total Salary"),
    avg("Salary").alias("Average Salary"),
    min("Salary").alias("Minimum Salary"),
    max("Salary").alias("Maximum Salary"),
    count("Name").alias("Total Count")
)
agg_df.show()

display(agg_df)

# COMMAND ----------

data = [("Aamir","IT",5000,"US"),
("Shazad","HR",6000,"UK"),
("Ali","Finance",7000,"US"),
("Ram","HR",8000,"CA"),
("Sham","IT",9000,"CA")]

columns = ["Name","Dept_Name","Dept_Salary","Country"]

df = spark.createDataFrame(data, columns)

df.show()

# COMMAND ----------

df.select(df.colRegex("`Dept_.*`")).show()

# COMMAND ----------

df.select(df.colRegex("`Name`")).show()

# COMMAND ----------

df.select(df.colRegex("`.*try.*`")).show()

# COMMAND ----------

# using column functions
display(df.columns)

# COMMAND ----------

for col in df.columns:
  print(col)

# COMMAND ----------

# DF to Json

import json

for row in df.collect():
    print(json.dumps(row.asDict()))

# COMMAND ----------

# Cache 
# Driver sends tasks to executors

# COMMAND ----------

data = [
    (1,'Ali',30),
    (2, 'Bob', 40),
    (3, 'Charlie', 50),
    (4, 'David', 60),
    (5, 'Eve', 70)
]

columns = ["id", "name", "salary"]

df = spark.createDataFrame(data, columns)
df.show()




# COMMAND ----------

#coalesce() Function Tutorial - Optimize Partitioning for Faster Spark Jobs
# partitions cannot check on serverless compute

# COMMAND ----------

data = [
    (1, "Aamir Shahzad", 35),
    (2, "Ali Raza", 30),
    (3, "Bob", 25),
    (4, "Lisa", 28)
]

columns = ["id", "name", "age"]

df = spark.createDataFrame(data, columns)
df.show()

# COMMAND ----------

print("Partitions before coalesce (approx):", len(df.inputFiles()))

# COMMAND ----------

df_coalesced = df.coalesce(1)

# COMMAND ----------

df_coalesced.show()

# COMMAND ----------

print("Partitions before coalesce (approx):", len(df.inputFiles()))

# COMMAND ----------

# collect () method : it collects everything from driver memory, which can lead to out of memory error
# python list 

# COMMAND ----------

collected_df = df.collect()

for row in collected_df:
    print(row)

# COMMAND ----------

# MAGIC %md ### Spark Window functions

# COMMAND ----------

data=[(1,'manish','india',10000),(2,'rani','india',50000),(3,'sunny','UK',5000),(4,'sohan','UK',25000),(5,'mona','india',10000)]

columns=['id','name','country','salary']

df =spark.createDataFrame(data,columns)
df.show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

window = Window.partitionBy("country").orderBy(desc("salary"))
df.withColumn("rn",row_number().over(window)).show()

# COMMAND ----------

window = Window.partitionBy("country").orderBy(desc("salary"))
df.withColumn("rn",rank().over(window)).show()

# COMMAND ----------

window = Window.partitionBy("country").orderBy(desc("salary"))
df.withColumn("rn",dense_rank().over(window)).show()

# COMMAND ----------

data = [
    ("Alice", 1200, "Q1"),
    ("Bob", 900, "Q1"),
    ("Charlie", 1500, "Q2"),
    ("David", 1700, "Q2"),
    ("Eva", 1100, "Q3"),
    ("Frank", 220, "Q3"),
    ("Grace", 1300, "Q4"),
    ("Helen", 2000, "Q4")
]


# COMMAND ----------


df = spark.createDataFrame(data, ["name", "sales", "quarter"])
df.show()

# COMMAND ----------

#get the empolyees who made sales in q3

from pyspark.sql.functions import *
df2 = df.filter(col("quarter") == "Q3").select("name")
df2.show()


# COMMAND ----------

# using whow function
from pyspark.sql.functions import *
from pyspark.sql.window import Window

window = Window.partitionBy("quarter").orderBy(desc("sales"))

ranked_df = df.withColumn("sales_rank", rank().over(window))

ranked_df.show()

q3_df = ranked_df.filter(col("quarter") == "Q3").select("name")
q3_df.show()


# COMMAND ----------

data = [
    (1,"Alice", 1200, "Q1"),
    (2,"Bob", 900, "Q1"),
    (3,"Charlie", 1500, "Q2"),
    (4,"David", 1700, "Q2"),
    (5,"Eva", 1100, "Q3"),
    (6,"Frank", 220, "Q3"),
    (7,"Grace", 1300, "Q4")
]

#Create DataFrame
df = spark.createDataFrame(data, ["id", "name", "sales", "quarter"])
df.show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

new_df = df.withColumn("previous",lag("name").over(Window.orderBy("id"))).withColumn("next",lead("name").over(Window.orderBy("id")))
new_df.show()


# COMMAND ----------

df2 = new_df.withColumn(
    "previous",
    when(
        col("id") % 2 == 1,
        coalesce(col("next"))
    ).otherwise(col("previous"))
).withColumn(
    "next",
    when(
        col("id") % 2 == 0,
        col("previous")
    ).otherwise(col("next"))
)

display(df2)
                                                           

   

# COMMAND ----------

df3 = df2.select("name", "previous")
df3.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table databricksansh.bronze.employee(id int, name string, manger_id int, salary int)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO databricksansh.bronze.employee (id, name, manger_id, salary)
# MAGIC VALUES
# MAGIC     (1, 'alice', NULL, 10000),
# MAGIC     (2, 'bob', 1, 8000),
# MAGIC     (3, 'charclie', 1, 7000),
# MAGIC     (4, 'david', 2, 9000),
# MAGIC     (5, 'eva', 2, 6000),
# MAGIC     (6, 'frank', 3, 5000),
# MAGIC     (7, 'grace', 3, 12000)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricksansh.bronze.employee

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT
# MAGIC     e.id,
# MAGIC     e.name,
# MAGIC     m.id,
# MAGIC     m.name,
# MAGIC     e.salary AS emp_sal,
# MAGIC     m.salary AS manger_salary
# MAGIC FROM databricksansh.bronze.employee e
# MAGIC inner JOIN databricksansh.bronze.employee m ON e.manger_id = m.id
# MAGIC where e.salary > m.salary
# MAGIC      
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,col

spark = SparkSession.builder.getOrCreate()

data = [
    (1, ["apple", "banana", "orange"]),
    (2, ["grape", "mango"]),
    (3, [])
]

df = spark.createDataFrame(data, ["id", "fruits"])
# df.show(truncate=False)

new_df = df.withColumn("fruit", explode(col("fruits"))).select("id","fruit")
new_df.show(truncate=False)


# COMMAND ----------

from pyspark.sql.functions import collect_list, col
df3 = new_df.groupBy("id").agg(collect_list(col("fruit")).alias("fruit_list"))

df3.show(truncate=False)

# COMMAND ----------

data = [ ['2024-01-01',20000,'HR'], ['2024-01-02',10000,'HR'],[ '2024-01-03',150000,'IT'], ['2024-01-04',100000,'IT'], ['2024-01-05',210000,'IT']] 
  
#define column names
columns = ['date', 'sales','dept'] 
  
#create dataframe using data and column names
df = spark.createDataFrame(data, columns) 
display(df)




# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

w = Window.partitionBy("dept").orderBy(col("date"))

new_df = df.withColumn("cumulative_sales", sum("sales").over(w))

new_df.show()



# COMMAND ----------

from pyspark.sql.functions import *

df.groupBy("dept").agg(sum("sales").alias("total_sales")).show()

df.groupBy("dept").sum("sales").alias("total_sales").show()

# COMMAND ----------

from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("StoreSales").getOrCreate()

# Sample data
data = [
    (1, 100),
    (1, 200),
    (1, 300),
    (2, 50),
    (3, 60)
]

# Column names
columns = ["store_id", "sales"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Show DataFrame
df.show()


# COMMAND ----------

from pyspark.sql.functions import sum

result_df = df.groupBy("store_id").agg(
    sum("sales").alias("total_sales")
)

display(result_df)


# COMMAND ----------

df.groupBy("store_id").sum("sales").explain()


# COMMAND ----------

# MAGIC %sql
# MAGIC create table workspace.default.tablea(id int);
# MAGIC create table workspace.default.tableb(id int);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.default.tablea;
# MAGIC select * from workspace.default.tableb;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into workspace.default.tablea values(1);
# MAGIC insert into workspace.default.tablea values(1);
# MAGIC insert into workspace.default.tablea values(2);
# MAGIC insert into workspace.default.tablea values(1);
# MAGIC insert into workspace.default.tablea values(3);
# MAGIC select * from workspace.default.tablea;
# MAGIC

# COMMAND ----------

data = {
  "event_id": "e1",
  "user": {
    "id": "u123",
    "location": {
      "country": "US",
      "city": "NY"
    }
  },
  "items": [
    {"product": "A", "price": 10},
    {"product": "B", "price": 20}
  ]
}


# COMMAND ----------

# MAGIC %scala
# MAGIC val schema = StructType(Seq(
# MAGIC   StructField("event_id", StringType),
# MAGIC   StructField("user", StructType(Seq(
# MAGIC     StructField("id", StringType),
# MAGIC     StructField("location", StructType(Seq(
# MAGIC       StructField("country", StringType),
# MAGIC       StructField("city", StringType)
# MAGIC     )))
# MAGIC   ))),
# MAGIC   StructField("items", ArrayType(
# MAGIC     StructType(Seq(
# MAGIC       StructField("product", StringType),
# MAGIC       StructField("price", DoubleType)
# MAGIC     ))
# MAGIC   ))
# MAGIC ))
# MAGIC
# MAGIC val df = spark.createDataFrame(schema=schema , data)
# MAGIC
# MAGIC df.show()
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


sales_data = [
    ('Product_A', 100, 150, 200),
    ('Product_B', 120, 80, 110),
    ('Product_C', 90, 130, 180),
    ('Product_D', 200, 160, 120)
]

schema = StructType([
    StructField('Product', StringType(), True),
    StructField('Jan_Sales', IntegerType(), True),
    StructField('Feb_Sales', IntegerType(), True),
    StructField('Mar_Sales', IntegerType(), True)
])

sales_data_df = spark.createDataFrame(sales_data, schema)

sales_data_df.show()

# COMMAND ----------

result = sales_data_df.unpivot(
    ids=["Product"],
    values=["Jan_Sales", "Feb_Sales", "Mar_Sales"],
    variableColumnName="Month",
    valueColumnName="Sales"
)
display(result)

# COMMAND ----------

data = [("John", "IT", 5000),
        ("Alice", "HR", 6000),
        ("Bob", "IT", 5500),
        ("Eve", "Finance", 7000),
        ("Charlie", "HR", 6500),
        ("David", "Finance", 7500),
        ("David", "Finance", 7500)]

columns = ["Name", "Department", "Salary"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)

df.show()

# COMMAND ----------

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

windowspec = Window.partitionBy("Department").orderBy(desc("Salary"))
df2 = df.withColumn(
    "sal_rank",
    row_number().over(windowspec)
)

display(df2)

# COMMAND ----------

new_df = df.groupBy("Department").agg(avg("Salary").alias("avg_sal")
)

new_df.show()


# COMMAND ----------

df2 = df.groupBy("Department").avg("Salary").alias("avg_sal")

df2.show()

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = [
    (1, 100, 2008, 10, 5000),
    (2, 100, 2009, 12, 5000),
    (7, 200, 2011, 15, 9000)
]

columns = ["sale_id", "product_id", "year", "quantity", "price"]

df = spark.createDataFrame(data, columns)
df.show()



# COMMAND ----------

df2 = df.filter(df.sale_id.isNull())
df2.show()

# COMMAND ----------

df.filter(df["sale_id"].isNull()).show()


# COMMAND ----------

df.filter(df.sale_id.isNotNull()).show()

# COMMAND ----------

df.filter(df["sale_id"].isNotNull()).show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

windowspec = Window.partitionBy("product_id").orderBy("year")

df = df.withColumn("sale_rank", row_number().over(windowspec))
df.show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

windowspec = Window.partitionBy("product_id").orderBy("year")

df = df.withColumn("sale_rank", rank().over(windowspec)).filter(df["sale_rank"] == 1)

df.show()

# COMMAND ----------

# Spark Coding Questions

# Write a solution to find all sales that occurred in the first year of each product that was sold. 

# Return all sales entries for that product in that year.

# Input:
# Sales table:
# +---------+------------+------+----------+-------+
# | sale_id | product_id | year | quantity | price |
# +---------+------------+------+----------+-------+
# | 1       | 100        | 2008 | 10       | 5000 |
# | 2       | 100        | 2009 | 12       | 5000 |
# | 7       | 200        | 2011 | 15       | 9000 |
# +---------+------------+------+----------+-------+
 


# with sale_cte as (

# select *,

# rank() over (partition by product_id order by year asc) as sale_rank

# from Sales)

# select * from sale_cte where sale_rank = 1;


# id , name , location
# 1, ABC, [Pune, BLR]
# 2, DEF, [NCR, HYD]
# 3, XYZ, [NCR]
 
 
# id , name , location
# 1, ABC, Pune
# 2, DEF, NCR
# 1, ABC, BLR
# 3, XYZ, NCR
# 2, DEF, HYD


# from pyspark.sql.functions import *
# from pyspark.sql.types import *


# spark = SparkSession.builder.appName("test").getOrCreate()


# schema = StructType([
#          StructField("id", IntegerType(),TRUE),
#          StructField("name",StringType(),TRUE),
#          StructField("location",ArrayType(Seq(StringType())),TRUE)])


# df = spark.createDataFrame(data=data, schema=schema)

# df.show()


# new_df = df.withColumn("new_location", explode(col("location")))

# new_df.show()

# new_df.write.format.("csv/delta").mode("overwrite").savAsPath("hdfs path")


# df = spark.read.format("csv").option("header","true").option("inferschema", "true")
#                .mode("Permissve/DropMalformed").load("path")
              
             
# new_df = df["id"].isNull()

# new_df.show()

# df2 = df["id"].isNotNull()

# df2.show()

# df3 = df["id"].fillna(0)

# df3.show()






# COMMAND ----------

# MAGIC %md ### Json Corrupt Record Example

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

df = spark.read \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .schema(schema) \
    .json("/Volumes/databricksansh/bronze/json_voulme/data.json")

df.show(truncate=False)


# COMMAND ----------

df.filter(df["id"].isNull()).show()

# COMMAND ----------

df.filter(df["id"].isNotNull()).show()

# COMMAND ----------

# MAGIC %md ### PySpark Scenario based coding Interview Questions for Data Engineers

# COMMAND ----------

data = [
    ("India", "Australia", "India"),
    ("England", "Pakistan", "England"),
    ("South Africa", "New Zealand", "New Zealand"),
    ("West Indies", "Sri Lanka", "Sri Lanka"),
    ("India", "Pakistan", "India"),
    ("Australia", "England", "Australia"),
    ("New Zealand", "Sri Lanka", "New Zealand"),
    ("South Africa", "West Indies", "South Africa"),
    ("India", "England", "India"),
    ("Pakistan", "Australia", "Pakistan"),
]

columns = ["Team_1", "Team_2", "Winner"]

df = spark.createDataFrame(data, columns)

df.show()

# COMMAND ----------

#The total number of matches played by each team

df.distinct().show()

# COMMAND ----------

from pyspark.sql.functions import *

df2 = df.select(
    col("Team_1").alias("Team"),
    "Winner"
).union(
    df.select(
        col("Team_1").alias("Team"),
        "Winner"
    )
).withColumn(
    "Winner_Flag",
    when(col("Team") == col("Winner"), 1).otherwise(0)
)

display(df2)




# COMMAND ----------

df2.groupBy("Team").agg(sum("Winner_Flag").alias("Total_Wins")).show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, to_date, regexp_replace,concat_ws,concat,when

#Define schema
schema = StructType([
    StructField("Id", IntegerType(), True),
    StructField("First name", StringType(), True),
    StructField("Last Name", StringType(), True),
    StructField("Expected DOJ", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Current CTC", IntegerType(), True)
])

#Create data
data = [
    (1, "Amit", "Sharma", "7/15/2023", "Engineering", 1250000),
    (2, "Priya", "Mehta", "5/20/2022", "IT", 980000),
    (3, "Rajesh", "Verma", "1/10/2024", "Engineering", 1500000),
    (4, "Neha", None, "9/25/2023", "IT", 1320000),
    (5, "Rohit", "Malhotra", "11/30/2022", "Engineering", 1070000)
]

#Create DataFrame
df = spark.createDataFrame(data, schema=schema)

df.display()#

# COMMAND ----------

# combine first name and second name

df2 = df.withColumn("Full Name", concat_ws(" ", col("First name"), col("Last Name")))

df2.show()



# COMMAND ----------

# offer a CTC for Engineering Dept give 25%

# offer a CTC for Engineering IT give 17%

df3 = df2.withColumn("Offered_CTC", when(col("Department") == "Engineering", col("Current CTC") * 1.25).when(col("Department") == "IT", col("Current CTC") * 1.17).otherwise(None))
display(df3)

# COMMAND ----------

# MAGIC %md ### find students with the same marks in math and chemistry  

# COMMAND ----------

# Create DataFrame
data = [
    (101, "Alice", "Math", 85),
    (101, "Alice", "Chemistry", 85),
    (101, "Alice", "Physics", 78),
    (102, "Bob", "Math", 90),
    (102, "Bob", "Physics", 88),
    (103, "Charlie", "Math", 75),
    (103, "Charlie", "Chemistry", 75),
    (104, "David", "Math", 88),
    (104, "David", "Chemistry", 88),
    (104, "David", "Physics", 95),
    (105, "Eve", "Chemistry", 91),
]

columns = ["Student_ID", "Name", "Subject", "Marks"]
df = spark.createDataFrame(data, columns)
df.show() 

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lag ,lead

windowspec = Window.partitionBy("Student_ID").orderBy("Subject")
df2 = df.withColumn("Previous_Marks", lag("Marks").over(windowspec)).withColumn("Next_Marks", lead("Marks").over(windowspec))

df2.show()

df3 = df2.filter(df2["Marks"]== df2["Previous_Marks"]).select("Student_ID","Name").distinct()

df3.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Employee SQL Table
# MAGIC CREATE OR REPLACE TABLE dev_env.bronze.employee (
# MAGIC     id INT,
# MAGIC     contact_number VARCHAR(15),
# MAGIC     mail_id VARCHAR(100),
# MAGIC     status VARCHAR(10) CHECK (status IN ('Active', 'Inactive'))
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert Sample Data (5–7 Rows)
# MAGIC INSERT INTO dev_env.bronze.employee VALUES
# MAGIC (1, '+91 9876543210', 'john.doe@gmail.com', 'Active'),
# MAGIC (2, '+1 9123456780',  'jane.smith@gmail.com', 'Inactive'),
# MAGIC (3, '+44 9988776655', 'mike.brown@gmail.com', 'Active'),
# MAGIC (4, '+91 9012345678', 'emily.white@gmail.com', 'Inactive'),
# MAGIC (5, '+61 9090909090', 'robert.black@gmail.com', 'Active'),
# MAGIC (6, '+1 8887776665',  'sara.green@gmail.com', 'Active');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_env.bronze.employee

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   id,
# MAGIC   concat(
# MAGIC     split(contact_number, ' ')[0], ' ',
# MAGIC     repeat('*', length(split(contact_number, ' ')[1]) - 4),
# MAGIC     right(split(contact_number, ' ')[1], 4)
# MAGIC   ) as masked_contact_number
# MAGIC from dev_env.bronze.employee

# COMMAND ----------

# MAGIC %sql
# MAGIC select id,
# MAGIC split(mail_id ,'@')[0] as name
# MAGIC from dev_env.bronze.employee
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select id,
# MAGIC split(split(mail_id ,'@')[0],'\\.')[0] as first_name,
# MAGIC split(split(mail_id ,'@')[0],'\\.')[1] as last_name
# MAGIC from dev_env.bronze.employee
# MAGIC
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %md ### find 3 conceutive days

# COMMAND ----------

data = [
    (1, '1/20/2025'),
    (1, '1/21/2025'),
    (1, '1/22/2025'),
    (1, '1/24/2025'),
    (2, '1/15/2025'),
    (2, '1/16/2025'),
    (2, '1/18/2025'),
    (3, '1/15/2025'),
    (3, '1/16/2025'),
    (3, '1/17/2025'),
    (3, '1/18/2025')
]

columns = ["emp_id", "login_date"]

df = spark.createDataFrame(data, columns)

df.show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import lead ,lag,cast

df2 = df.withColumn(
    "date_col",
    col("login_date").cast("date")
)

df2.printSchema()


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import lead ,lag,cast,to_date

df2 = df.withColumn(
    "date_col",
    to_date(
        col("login_date"),
        "M/d/yyyy"
    )
)
from pyspark.sql.window import Window
from pyspark.sql.functions import lead ,lag,cast,to_datedf2.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lead ,lag,cast,to_date,col

w = Window.partitionBy("emp_id").orderBy("date_col")

df3 = df2.withColumn("row_num", row_number().over(w))\
         .withColumn("group_id",expr("date_sub(date_col, row_num)"))

result = df3.groupBy("emp_id","group_id").count().filter(col("count")>=3).select("emp_id").distinct()

display(result)

# COMMAND ----------

# MAGIC %md ### Crypto Price Trend Analysis with PySpark | Stock Market & E-commerce Use Cases 2025

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Sample Data creation
data = [
    (1, "Bitcoin", "2024-02-01", 430, 435),
    (1, "Bitcoin", "2024-02-02", 435, 440),
    (1, "Bitcoin", "2024-02-03", 440, 439),
    (1, "Bitcoin", "2024-02-04", 445, 430),
    (2, "Ethereum", "2024-02-01", 320, 325),
    (2, "Ethereum", "2024-02-02", 325, 330),
    (2, "Ethereum", "2024-02-03", 330, 335),
    (2, "Ethereum", "2024-02-04", 335, 340),
    (3, "Solana", "2024-02-01", 120, 125),
    (3, "Solana", "2024-02-02", 125, 130),
    (3, "Solana", "2024-02-03", 130, 135),
    (3, "Solana", "2024-02-04", 135, 140),
]
columns = ["Coin_ID", "Coin_Name", "Date", "Open_Price", "Close_Price"]
df = spark.createDataFrame(data, columns)
df.printSchema()
df.show()

# COMMAND ----------

# covert date 
from pyspark.sql.functions import to_date
df2 = df.withColumn("Date", F.to_date(F.col("Date"), "yyyy-MM-dd"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lead ,lag,cast,to_date,col

w = Window.partitionBy("Coin_ID").orderBy("Date")

df3 = df2.withColumn("Prev_Close_Price_1", lag("Close_Price", 1).over(w))\
         .withColumn("Prev_Close_Price_2", lag("Close_Price", 2).over(w))

display(df3)      

# COMMAND ----------

#Filter coins that had a price increase for 3 consecutive days
from pyspark.sql.window import Window
from pyspark.sql.functions import lead ,lag,cast,to_date,col

df_result = df3.filter(
    (col("Close_Price") > col("Prev_Close_Price_1")) &
    (col("Prev_Close_Price_1") > col("Prev_Close_Price_2"))
).select("Coin_ID", "Coin_Name").distinct()

display(df_result)



# COMMAND ----------

# MAGIC %md ###  find employees who only hold a Laptop in an Active state using PySpark.

# COMMAND ----------

# Sample Data
data = [
    (101, "Laptop", "2024-01-15", "Active"),
    (101, "Mobile", "2024-02-10", "Active"),
    (102, "Laptop", "2024-03-05", "Active"),
    (103, "Laptop", "2023-12-20", "Inactive"),
    (103, "Tab", "2024-02-25", "Active"),
    (104, "Mobile", "2024-04-10", "Active"),
    (104, "Laptop", "2024-01-30", "Inactive"),
    (105, "Laptop", "2024-03-15", "Active"),
    (105, "Tab", "2024-02-05", "Inactive"),
    (105, "Mobile", "2024-01-20", "Inactive"),
]

columns = ["employee_id", "device_type", "issued_date", "status"]
df = spark.createDataFrame(data, columns)
df.printSchema()
df.show()


# COMMAND ----------

# MAGIC %md ### Step 1: Get employees who have an active Laptop

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lead ,lag,cast,to_date,col
df2 = df.filter((col("device_type") == "Laptop") & (col("status") == "Active")).select("employee_id")

df2.show()

# COMMAND ----------

# MAGIC %md ### Get employees who have any other device (Tab/Mobile)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lead ,lag,cast,to_date,col

df3 = df.filter(
    (col("device_type").isin("Tab", "Mobile")) &
    (col("status") == "Active")
).select("employee_id")


df3.show()

# COMMAND ----------

# MAGIC %md ### Exclude employees who have other devices

# COMMAND ----------

from pyspark.sql.functions import *

df4 = df2.join(df3, "employee_id", "leftanti")
df4.show()


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Department table
# MAGIC CREATE TABLE workspace.default.dept (
# MAGIC     id INT PRIMARY KEY,
# MAGIC     dep_name VARCHAR(50)
# MAGIC );
# MAGIC
# MAGIC -- Create Employee table
# MAGIC CREATE TABLE workspace.default.emp (
# MAGIC     Id INT PRIMARY KEY,
# MAGIC     dep_id INT,
# MAGIC     name VARCHAR(50),
# MAGIC     salary INT,
# MAGIC     FOREIGN KEY (dep_id) REFERENCES dept(id)
# MAGIC );
# MAGIC
# MAGIC -- Insert data into Department
# MAGIC INSERT INTO workspace.default.dept (id, dep_name) VALUES
# MAGIC (101, 'IT'),
# MAGIC (102, 'HR'),
# MAGIC (103, 'Marketing');
# MAGIC
# MAGIC -- Insert data into Employee
# MAGIC INSERT INTO workspace.default.emp (Id, dep_id, name, salary) VALUES
# MAGIC (1, 101, 'Alice', 90000),
# MAGIC (2, 101, 'Bob', 85000),
# MAGIC (3, 101, 'Charlie', 90000),
# MAGIC (4, 102, 'David', 75000),
# MAGIC (5, 102, 'Eva', 70000),
# MAGIC (6, 102, 'Frank', 65000),
# MAGIC (7, 103, 'Grace', 60000),
# MAGIC (8, 103, 'Henry', 60000),
# MAGIC (9, 103, 'Ian', 55000);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.default.emp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.default.emp;
# MAGIC select * from workspace.default.dept;

# COMMAND ----------

# MAGIC %md ### find second highest salary in each dept

# COMMAND ----------

# MAGIC %sql
# MAGIC with rankedsalary as(
# MAGIC   select e.Id,e.name,e.dep_id,e.salary,
# MAGIC   dense_rank () over (partition by dep_id order by salary desc) as salary_rank
# MAGIC   from workspace.default.emp e
# MAGIC   join workspace.default.dept d
# MAGIC   on e.dep_id = d.id
# MAGIC )
# MAGIC select * from rankedsalary where salary_rank = 2
# MAGIC
# MAGIC   
# MAGIC
# MAGIC
# MAGIC  

# COMMAND ----------

from pyspark.sql import Row

A = spark.createDataFrame([
    Row(id_A='1'),
    Row(id_A='1'),
    Row(id_A='2'),
    Row(id_A=''),
    Row(id_A=''),
    Row(id_A=None)
])

B = spark.createDataFrame([
    Row(id_B='1'),
    Row(id_B='2'),
    Row(id_B='3'),
    Row(id_B='3'),
    Row(id_B=''),
    Row(id_B=None)
])

# COMMAND ----------

inner_df = A.join(B, A.id_A == B.id_B, 'inner')
display(inner_df)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC

# COMMAND ----------

left_df = A.join(B, A.id_A == B.id_B, 'left')
display(left_df)

right_df = A.join(B, A.id_A == B.id_B, 'right')
display(right_df)

outer_df = A.join(B, A.id_A == B.id_B, 'outer')
display(inner_df)

# COMMAND ----------

# MAGIC %md ### Contact number contains country code + mobile number

# COMMAND ----------

data = [
    (1, "+91 9876543210", "john.doe@gmail.com", "Active"),
    (2, "+1 9123456780", "jane.smith@gmail.com", "Inactive"),
    (3, "+44 9988776655", "mike.brown@gmail.com", "Active"),
    (4, "+91 9012345678", "emily.white@gmail.com", "Inactive"),
    (5, "+61 9090909090", "robert.black@gmail.com", "Active")
]

columns = ["id", "contact_number", "mail_id", "status"]

employee_df = spark.createDataFrame(data, columns)

employee_df.show(truncate=False)

# COMMAND ----------

#Country Dictionary (Mapping Source)
country_dict = {
    "+91": "India",
    "+1": "United States",
    "+44": "United Kingdom",
    "+61": "Australia"
}    

# COMMAND ----------

from pyspark.sql.functions import create_map, lit , split,col
from itertools import chain

country_map = create_map([lit(x) for x in chain(*country_dict.items())])


# COMMAND ----------

df_transformed = employee_df.withColumn("country_code", split(col("contact_number"), " ")[0])
df_transformed.show(truncate=False)


# COMMAND ----------

new_df = df_transformed.withColumn("country", country_map[col("country_code")])
new_df.show(truncate=False)


# COMMAND ----------

# MAGIC %md ### calculate percentage by each student

# COMMAND ----------

data1=[(1,"Steve"),(2,"David"),(3,"John"),(4,"Shree"),(5,"Helen")]

data2=[(1,"SQL",90),(1,"PySpark",100),(2,"SQL",70),(2,"PySpark",60),(3,"SQL",30),(3,"PySpark",20),(4,"SQL",50),(4,"PySpark",50),(5,"SQL",45),(5,"PySpark",45)]

schema1=["Id","Name"]

schema2=["Id","Subject","Mark"]

df1=spark.createDataFrame(data1,schema1)

df2=spark.createDataFrame(data2,schema2)

display(df1)

display(df2)

# COMMAND ----------

df_join=df1.join(df2,df1.Id==df2.Id).drop(df2.Id)
display(df_join)

# COMMAND ----------

from pyspark.sql.functions import *

df_per=df_join.groupBy('Id','Name').agg((sum('Mark')/count('*')).alias('Percentage')
)
display(df_per)

# COMMAND ----------

from pyspark.sql.functions import *

new_df = df_per.select('*',
                 when(df_per.Percentage >= 70, 'A')
                .when(df_per.Percentage >= 60, 'B')
                .when(df_per.Percentage >= 50, 'C')
                .when(df_per.Percentage >= 40, 'D')
                .when(df_per.Percentage >= 30, 'E')
                .when(df_per.Percentage >= 20, 'F').alias('Result'))

display(new_df)


# COMMAND ----------

data1=[(1,"A",1000,"IT"),(2,"B",1500,"IT"),(3,"C",2500,"IT"),(4,"D",3000,"HR"),(5,"E",2000,"HR"),(6,"F",1000,"HR")
       ,(7,"G",4000,"Sales"),(8,"H",4000,"Sales"),(9,"I",1000,"Sales"),(10,"J",2000,"Sales")]
schema1=["EmpId","EmpName","Salary","DeptName"]
df=spark.createDataFrame(data1,schema1)
display(df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window 

windowSpec = Window.partitionBy("DeptName").orderBy(col("Salary").desc())

ranked_df = df.withColumn("sales_rank", dense_rank().over(windowSpec)).filter(col("sales_rank") <= 1)

display(ranked_df)




# COMMAND ----------

#empoloyee dataframe
data1=[(100,"Raj",None,1,"01-04-23",50000),
       (200,"Joanne",100,1,"01-04-23",4000),(200,"Joanne",100,1,"13-04-23",4500),(200,"Joanne",100,1,"14-04-23",4020)]
schema1=["EmpId","EmpName","Mgrid","deptid","salarydt","salary"]
df_salary=spark.createDataFrame(data1,schema1)
display(df_salary)


#department dataframe
data2=[(1,"IT"),
       (2,"HR")]
schema2=["deptid","deptname"]
df_dept=spark.createDataFrame(data2,schema2)
display(df_dept)


# COMMAND ----------

# convert date format
from pyspark.sql.functions import to_date

df_salary=df_salary.withColumn("Newsalarydt",to_date(col("salarydt"),"dd-MM-yy"))
display(df_salary)


# COMMAND ----------

from pyspark.sql.functions import *
df2 = df_salary.join(df_dept, on='deptid', how='left')
df2.display()


# COMMAND ----------

from pyspark.sql.functions import *
df3 = df2.groupBy(
    'deptname',
    'EMpName',
    col("EmpName").alias('ManagerName'),
    year('Newsalarydt').alias('SalYear'),
    month('Newsalarydt').alias('SalMonth')
).agg(
    sum('salary').alias('Total_Salary')
)

display(df3)










# COMMAND ----------

df_salary.rdd.getNumPartitions()

# COMMAND ----------

df1 = df_salary.repartition(2)
df1.rdd.getNumPartitions()


# COMMAND ----------

# MAGIC %md ### modifiedbefore and modifiedafter

# COMMAND ----------

# MAGIC %sh
# MAGIC date

# COMMAND ----------

from datetime import datetime

files = dbutils.fs.ls("/Volumes/databricksansh/bronze/emp_volume/emp.csv")
filtered_files = [
    f.path
    for f in files
    if datetime.fromtimestamp(f.modificationTime / 1000) < datetime(2026, 2, 18)
]

df = spark.read.format("csv") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load(filtered_files)

display(df)

# COMMAND ----------

# MAGIC %md ### Schema Comparison

# COMMAND ----------

data1=[(1,"Ram","Male",100),(2,"Radhe","Female",200),(3,"John","Male",250)]
data2=[(101,"John","Male",100),(102,"Joanne","Female",250),(103,"Smith","Male",250)]
data3=[(1001,"Maxwell","IT",200),(2,"MSD","HR",350),(3,"Virat","IT",300)]
schema1=["Id","Name","Gender","Salary"]
schema2=["Id","Name","Gender","Salary"]
schema3=["Id","Name","DeptName","Salary"]
df1=spark.createDataFrame(data1,schema1)
df2=spark.createDataFrame(data2,schema2)
df3=spark.createDataFrame(data3,schema3)
display(df1)
display(df2)
display(df3)

# COMMAND ----------

#missing Columns
if df1.schema==df3.schema:
    print('Schema Matched')
else:
    print('Schema not matched')

print(list(set(df1.schema)-set(df3.schema)))
print(list(set(df3.schema)-set(df1.schema)))

# COMMAND ----------

#collect all the columns
allcol=df1.columns+df3.columns
uniquecol=list(set(allcol))
print(uniquecol)

# COMMAND ----------

df = spark.read.option(
    "header", "true"
).option(
    "recursiveFileLookup", "true"
).csv("/Volumes/databricksansh/bronze/country_volume")
display(df)

# COMMAND ----------

# MAGIC %md ### pepsico total sales count

# COMMAND ----------

from pyspark.sql.types import *
data=[(1,'2024-01-01',"I1",10,1000),(2,"2024-01-15","I2",20,2000),(3,"2024-02-01","I3",10,1500),(4,"2024-02-15","I4",20,2500),(5,"2024-03-01","I5",30,3000),(6,"2024-03-10","I6",40,3500),(7,"2024-03-20","I7",20,2500),(8,"2024-03-30","I8",10,1000)]
schema=["SOId","SODate","ItemId","ItemQty","ItemValue"]
df1=spark.createDataFrame(data,schema)
df1.show()

# COMMAND ----------

df1 = df1.withColumn("SODate", df1.SODate.cast("date"))
df1.printSchema()


# COMMAND ----------

from pyspark.sql.functions import *
df2=df1.select(month(df1.SODate).alias('Month'),year(df1.SODate).alias('Year'),df1.ItemValue)
df2.show()

# COMMAND ----------

df3=df2.groupBy(df2.Month,df2.Year).agg(sum(df2.ItemValue).alias('TotalSale'))
df3.show()

# COMMAND ----------

from pyspark.sql.window import *
df4=df3.select('*',lag(df3.TotalSale).over(Window.orderBy(df3.Month,df3.Year)).alias('PrevSale'))
df4.show()

# COMMAND ----------

display(df4.select('*',(df4.TotalSale-df4.PrevSale)*100/df4.TotalSale).alias('PercentChange'))