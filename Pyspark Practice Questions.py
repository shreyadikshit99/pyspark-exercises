# Databricks notebook source
data = [
  ('Shreya',),
  ('Deve',)
]
col = ['name',]
df = spark.createDataFrame(data,col)

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import col, length

# COMMAND ----------

df = df.withColumn("word_count", length(df.name))

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to compute difference of differences between consecutive numbers of a column?

# COMMAND ----------

data = [
  ("Shreya", 24, 50000),
  ("Deve",21, 60000),
  ("Shubhe", 23, 60000)
]
col = ["name", "age", "salary"]

# COMMAND ----------

df = spark.createDataFrame(data, col)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lag, monotonically_increasing_id, isnull, to_date

# COMMAND ----------

df = df.withColumn("id",monotonically_increasing_id())
df.show()

# COMMAND ----------

w = Window.orderBy("id")

# COMMAND ----------

df = df.withColumn("prev", lag(df.salary).over(w))
df.show()

# COMMAND ----------

df = df.withColumn("diff", when(isnull(df.prev-df.salary),0).otherwise(df.prev-df.salary)).drop("id")
df.show()

# COMMAND ----------

data = [("2023-05-18","01 Jan 2010",), ("2023-12-31", "01 Jan 2010",)]
df = spark.createDataFrame(data, ["date_str_1", "date_str_2"])

df.show()

# COMMAND ----------

df = df.withColumn("date_str1_converted", to_date(df.date_str_1, 'yyyy-MM-dd')).withColumn("date_str2_converted", to_date(df.date_str_2,'dd MMM yyyy')).drop("date_str_1").drop("date_str_2")
df.show()

# COMMAND ----------

from pyspark.sql.functions import dayofmonth, weekofyear, dayofweek, dayofyear

# COMMAND ----------

df = df.withColumn("dayofyear",dayofyear(df.date_str1_converted)).withColumn("dayofweek",dayofweek(df.date_str1_converted)).withColumn("weekofyear",weekofyear(df.date_str1_converted)).withColumn("dayofmonth",dayofmonth(df.date_str1_converted))

# COMMAND ----------

df.show()

# COMMAND ----------


