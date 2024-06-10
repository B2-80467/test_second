# Databricks notebook source
# 1. data = [ 
#     ("Alice", "Engineering", 100000, 5, "2019-01-15"), 
#     ("Bob", "Engineering", 95000, 4, "2020-03-22"), 
#     ("Charlie", "HR", 70000, 2, "2018-07-30"), 
#     ("David", "HR", 60000, 3, "2019-10-10"), 
#     ("Eve", "Marketing", 85000, 4, "2021-05-15"), 
#     ("Frank", "Marketing", 80000, 3, "2017-12-01"), 
#     ("Grace", "Finance", 90000, 5, "2016-04-25"), 
#     ("Heidi", "Finance", 75000, 3, "2018-02-20"), 
#     ("Ivan", "Engineering", 95000, 4, "2020-12-18"), 
#     ("Judy", "Engineering", 92000, 2, "2017-09-11") 
# ] 
# columns = ["Name", "Department", "Salary", "Experience", "JoiningDate"] 
#  List all employees who have a salary higher than the average salary of their 
# department. 
#  Identify the most recent joiner in each department. 
#  Find the median salary of employees in the company. 
#  List the names of employees along with their salary who have the same salary as 
# another employee. 
#  Find the employee with the second highest salary. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### from first dataset five questions

# COMMAND ----------

data = [ 
    ("Alice", "Engineering", 100000, 5, "2019-01-15"), 
    ("Bob", "Engineering", 95000, 4, "2020-03-22"), 
    ("Charlie", "HR", 70000, 2, "2018-07-30"), 
    ("David", "HR", 60000, 3, "2019-10-10"), 
    ("Eve", "Marketing", 85000, 4, "2021-05-15"), 
    ("Frank", "Marketing", 80000, 3, "2017-12-01"), 
    ("Grace", "Finance", 90000, 5, "2016-04-25"), 
    ("Heidi", "Finance", 75000, 3, "2018-02-20"), 
    ("Ivan", "Engineering", 95000, 4, "2020-12-18"), 
    ("Judy", "Engineering", 92000, 2, "2017-09-11") 
] 

columns = ["Name", "Department", "Salary", "Experience", "JoiningDate"] 

employee_df = spark.createDataFrame(data=data,schema=columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  List all employees who have a salary higher than the average salary of their department. 

# COMMAND ----------

#grouping
# from pyspark.sql.functions import avg
dep_wise_average_salary = (employee_df.groupBy("Department")
                 .agg(avg("Salary").alias("averageSalary"))   
      )


  

# from pyspark.sql.window import Window

display(dep_wise_average_salary)

    

# COMMAND ----------

from pyspark.sql.functions import col
joined_df = (employee_df.join(dep_wise_average_salary , employee_df["Department"] == dep_wise_average_salary["Department"] , "inner")
                        .filter(col("Salary") > col("averageSalary")))

# COMMAND ----------

list_of_emp_names = joined_df.select("Name")

# COMMAND ----------

display(list_of_emp_names)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Identify the most recent joiner in each department.

# COMMAND ----------

from pyspark.sql.functions import to_date
employee_df_correct_datatypes=(employee_df.withColumn("JoiningDate" , to_date("JoiningDate")))

# COMMAND ----------

from pyspark.sql.window import Window 
from pyspark.sql.functions import desc , col , row_number , asc
window = Window.partitionBy("Department").orderBy(desc(col("JoiningDate")))
final = (employee_df.withColumn("row_number" , row_number().over(window))
                    .where("row_number == 1")
                    
         )
display(final)


# COMMAND ----------

# MAGIC %md
# MAGIC ###  Find the median salary of employees in the company. 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import median
median_salary = (employee_df.groupBy()
                            .agg(median(col("Salary")).alias("median_salary")) )

display(median_salary)                            

# COMMAND ----------

# MAGIC %md
# MAGIC ###  List the names of employees along with their salary who have the same salary as  another employee.

# COMMAND ----------

clone_copy_of_employee = employee_df

final = 

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Find the employee with the second highest salary.

# COMMAND ----------

from pyspark.sql.window import Window 
from pyspark.sql.functions import desc , col , dense_rank , asc
window_ = Window.partitionBy().orderBy(desc(col("Salary")))

df = (employee_df.withColumn("dense_rank" , dense_rank().over(window_))
                .where("dense_rank == 2")
                .drop(col("dense_rank"))
                .show()
            )

# COMMAND ----------

sales_data = [ 
    ("2024-01-01", 1, 10, 100), 
    ("2024-01-01", 2, 5, 200), 
    ("2024-01-02", 1, 8, 100), 
    ("2024-01-02", 2, 7, 200), 
] 
sales_columns = ["date", "product_id", "quantity", "price"] 
sales_df = spark.createDataFrame(sales_data, schema=sales_columns) 
customer_data = [ 
    (1, "diksha", 35), 
    (2, "vansh", 25), 
    (3, "adhyan", 45) 
] 
customer_columns = ["customer_id", "name", "age"] 
customers_df = spark.createDataFrame(customer_data, schema=customer_columns) 
order_data = [ 
    (101, 1, 1, 10), 
    (102, 1, 2, 5), 
    (103, 2, 1, 8), 
    (104, 3, 2, 7), 
] 
order_columns = ["order_id", "customer_id", "product_id", "quantity"] 
orders_df = spark.createDataFrame(order_data, schema=order_columns) 
 




#  Write a PySpark script to pivot sales_df so that each row represents a date, each 
# column represents a product_id, and the cell values are the total quantity sold on 
# that date. 


# COMMAND ----------

# MAGIC %md
# MAGIC ###  Write a PySpark script to filter sales_df to only include sales from "2024-01-01" 
# MAGIC

# COMMAND ----------

(
    sales_df.where("date == '2024-01-01'").show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Write a PySpark script to join customers_df with orders_df on customer_id and include customer names in the result 

# COMMAND ----------

joined_df_of_customer_df_and_order_df = ( customers_df.join(orders_df , customers_df["customer_id"] == orders_df["customer_id"] , "inner")
                                         .show()

                                        )

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Write a PySpark script to group sales_df by product_id and calculate the total 
# MAGIC ### quantity sold for each product. 

# COMMAND ----------

from pyspark.sql.functions import sum
total_quantity_sold_per_product = ( sales_df.groupBy("product_id")
                                            .agg(sum(col("quantity")).alias("total_sales"))
                                            .show()

)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Write a PySpark script using window functions to calculate the running total of the 
# MAGIC ### quantity sold for each product_id over time in sales_df 

# COMMAND ----------

sales__df = ( sales_df.withColumn("date" , to_date("date")) )


# COMMAND ----------

window = Window.partitionBy("product_id").orderBy("date").rowsBetween(Window.unboundedPreceding , Window.currentRow)

running_total = ( sales__df.withColumn("running_total" , sum(col("quantity")).over(window)) ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  write a PySpark script to identify days in sales_df where the total revenue was greater than 1000. 

# COMMAND ----------



total_revenue_df = ( sales__df.withColumn("total_revenue" , col("quantity") * col("price"))
                              .where("total_revenue > 1000")
                        )

display(total_revenue_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### write a PySpark script to calculate the total revenue generated by each customer  based on the joined orders_df and sales_df. 

# COMMAND ----------

display(sales_df)

# COMMAND ----------

customer_df = ( sales_df.drop("quantity").join(orders_df, on="product_id", how="inner")
                                         .withColumn("revenue" , col("price") * col("quantity"))  
                                         .groupBy("customer_id")
                                         .agg(sum("revenue").alias("total_revenue")) 
                        
)
display(customer_df)

# COMMAND ----------

display(orders_df)

# COMMAND ----------

data = [(1, 'vikas','20000'),(2,'mahavat','40000'),(3, 'jhon','25000'),(4, 
'rahul','30000'),(5, 'vinod','33000'), (6,'junai','52000'), (7,'arjun','18000'), 
(8,'rakesh','70000'), (9,'mahima','35000'), (10,'gulshan','62000')] 

from pyspark.sql.types import StructType , StructField , IntegerType , StringType
schema_ = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("salary", StringType(), True)
])

# COMMAND ----------

employee_dataframe = spark.createDataFrame(data=data , schema=schema_)

# COMMAND ----------

# 3. Task 1  
# a. Show the table data vertically  
display(employee_dataframe)
# b. Show the content of only 4 rows 
employee_dataframe.show(n=4) 
# c. Show staring 3 characters of each columns 
from pyspark.sql.functions import substring
three_char_df= ( employee_dataframe.withColumn("id_three_char", col("id").substr(1,3))
                      .withColumn("name_three_char", col("name").substr(1,3))
                      .withColumn("salary_three_char", col("salary").substr(1,3))
                      .drop("id","name","salary")
                      .show()
                    )


# COMMAND ----------

# 4. Task 2  
# o Change the datatype of salary column
changed_datatype_salary = employee_dataframe.withColumn("salary_changed" , col("salary").cast("long")).printSchema() 
# o Add a column increment having value 15% of the salary column 

salary_increment = ( employee_dataframe.withColumn("salary" , col("salary").cast("long"))
                                       .withColumn("15_percent-increment" , col("salary")* 0.15 )  
                     )
display(salary_increment)                     


# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Task 3 - create a column putting values where salary smaller than 20000 be low , between 
# MAGIC ### 20000 to 50000 be mid , greater than 50000 be high  by using when

# COMMAND ----------

from pyspark.sql.functions import when

categorical_emp_df = ( employee_dataframe.withColumn("category" , when(col("salary") > 50000 , "high")
                                                                  .when(col("salary").between(20000, 50000), "mid")
                                                                  .when(col("salary") < 20000 , "low")
                                                                  
                                                                  )
                      )

display(categorical_emp_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Task 4- Filter the rows whose name start with ‘v’ , contains ‘a’ at the last second position and contains ‘j’, ’e’, ’u’ using ‘where’ function 

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Task 5- create a column short_name by extracting the first 3 alphabets from the name column 

# COMMAND ----------

short_name_df = employee_dataframe.withColumn("name_three_char", col("name").substr(1,3))
display(short_name_df)

# COMMAND ----------

# . Write schema for  
from pyspark.sql.types import DoubleType
data = [(1, ('vikas','yadav'),20000),(2,('mahavat','singh'),40000),
         (3, ('jhon','merchant'),25000),(4, ('rahul','verma'),30000), 
        (5, ('vinod','devangan'),33000)] 
 
sc = StructType([
    StructField("emp_id" , IntegerType() , True),
    StructField("struct", StructType([
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True)

    ])),
    StructField("salary", IntegerType() , True)
]) 
# schema_ = "id int, struct< first_name , last_name >, slary long" 
emp_schema_given_df = spark.createDataFrame(data , schema= sc)

# COMMAND ----------

emp_schema_given_df.printSchema()

# COMMAND ----------

# 9. Create a row  having array of both number a and b 
data =[(1,2),(3,4)] 
schema_ = ['a','b'] 
df= spark.createDataFrame(data,schema_) 

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # ## create a column having value for weather skills column contain java or not  
# MAGIC # ##explode the skills column into two separate column primary_skill and  secondary_skill 

# COMMAND ----------

d3=[(1,'Raghav',['Excel','azure']),(2,'Sohail',['python','AWS']),(3,'Raghav',['java','GCP'])] 
schema2 =['id','name','skills'] 

emp_skills = spark.createDataFrame(data=d3 , schema=schema2)

# COMMAND ----------

emp_skills.printSchema()

# COMMAND ----------

from pyspark.sql.functions import array_contains
contains_skill_or_not = ( emp_skills.withColumn("have_or_not" , array_contains(col("skills") , "java") )
                         
                         )
display(contains_skill_or_not)                         

# COMMAND ----------

from pyspark.sql.functions import explode
emp_skills.select("*" , explode(col("skills")).alias("sk") ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### o Write schema for the table  
# MAGIC

# COMMAND ----------

data = [(1, 
'vikas',{'hair':'black','eye':'brown'}),(2,'mahavat',{'hair':'brown','eye':'blue'}),(
 3, 'jhon',{'hair':'tan','eye':'green'}),(4, 
'rahul',{'hair':'grey','eye':'brown'}),(5, 'vinod',{'hair':'red','eye':'red'})] 

from pyspark.sql.types import MapType , IntegerType , StringType , StructType , StructField
schema_ = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("properties", MapType(
        StringType(), StringType()
    ))
])
 


# COMMAND ----------

person_df = spark.createDataFrame(data , schema=schema_)
person_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create as separate column extracting the hair values in it  
# MAGIC

# COMMAND ----------

# display(person_df)
hair_value_df = person_df.withColumn("hair_color", col("properties").getItem("hair"))
display(hair_value_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create two separate column having the key and value in them respectively 
# MAGIC

# COMMAND ----------

person_with_properties = (person_df.withColumn("hair_color", col("properties").getItem("hair"))
                                   .withColumn("eye_color", col("properties").getItem("eye"))

                          )
display(person_with_properties)                          

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a separate column and extract only the keys 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import map_keys , map_values
keys_df = person_df.withColumn("keys" , map_keys(col("properties"))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a separate column and extract only the values 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import map_keys , map_values
keys_df = person_df.withColumn("keys" , map_values(col("properties"))).show()

# COMMAND ----------


