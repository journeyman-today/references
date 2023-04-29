
  

`from pyspark.sql.functions import col, when, count, asc, desc, concat, desc`

  

## Load data

`mydata = spark.read.csv('/home/user/projects/in_data/MyData.csv', header=True)`

  

## Limiting rows, e.g. 15%

`mydata_limited = mydata.limit(int(mydata.count()*0.15))`

  

## No. of rows in a dataframe

`mydata_limited.count()`

  

## View content of dataframe

`mydata_limited.show()`
NOTE: When assigning the result to a dataframe make sure the show() function is not used.

### Dont truncate contents of long columns

`mydata_limited.show(truncate=False)`

### View data for only selected columns

`mydata_limited.select(["firstname", "lastname"]).show(truncate=False)`

  

## Get all distinct values in a column

`temp_data = mydata_limited.select(col("regno")).distinct()`

### Get count instead

`mycount = mydata_limited.select(col("regno")).distinct().count()`

  

## Order by a Column

`temp_data = mydata_limited.orderBy(asc("firsname"))`

Replace `asc` by `desc` for descending order

  

## Filter on a column value

`temp_data = mydata_limited.filter(mydata_limited.regno == "a0102")`

  

## Create new column

`temp_data = mydata_limited.withColumn("lastname_regno", concat(col("lastname"), lit("_"), col("regno")))`
## Create new column based on a condition
`temp_data = mydata_limited\
                .withColumn("lastname_regno", when(mydata_limited["regno"] == "a0000", "NO_NAME_GUY")\
                                            .otherwise(concat(col("lastname"), lit("_"), col("regno")))`
  

## Chain multiple operations together and show result on screen
~~~
mydata_limited.withColumn("lastname_regno", concat(col("lastname"), lit("_"), col("regno")))\

.filter(mydata_limited.lastname == "Wayne")\

.select(["firsname", "lastname_regno"])\

.show(truncate=False)
~~~


## Chain multiple operations together and assign result to another dataframe

~~~
temp_data = mydata_limited.withColumn("lastname_regno", concat(col("lastname"), lit("_"), col("regno")))\

.filter(mydata_limited.regno == "a0102")\

.select(["firsname", "lastname_regno"])
~~~

## Group by a column to find number of rows for each value of that column
`mydata_limited.groupBy('city').count().show()`

## Joining 2 dataframes
~~~
temp_data = mydata_limited.withColumnRenamed("regno", "regno2")\
.join(registrations, col("regno") == col("regno"), "inner")\
.drop("regno2")
~~~
Joining mydata_limited and registsration data frames. `regno` is renamed in one dataframe before join since they are named the same in both dataframes and then dropped after the join.
