# Databricks notebook source
# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/tables/circuits.csv")
df.show()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuit_schema = StructType(fields = [StructField("circuitId", IntegerType(),False),
                                      StructField("circuitRef", StringType(), True),
                                      StructField("name", StringType(), True),
                                      StructField("location", StringType(), True),
                                      StructField("country", StringType(), True),
                                      StructField("lat", DoubleType(), True),
                                      StructField("lng", DoubleType(), True),
                                      StructField("alt", IntegerType(), True),
                                      StructField("url", StringType(), True)                              
])

# COMMAND ----------

select_df = df.select("circuitId","circuitRef", "name", "location", "country", "lat" ).show()

# COMMAND ----------

#renaming columns
from pyspark.sql.functions import col
select_df = df.select(col("circuitId"),col("circuitRef"), col("name"), col("location"), col("country"), col("lat").alias("latitude"), col("lng").alias("longitude"), col("alt").alias("altitude"))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
circuit_df = select_df.withColumn("ETL_loaddate", current_timestamp())\
.withColumn("Layer", lit("Raw"))

# COMMAND ----------

circuit_df.show()

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/FileStore/tables/raw/

# COMMAND ----------

circuit_df = circuit_df.withColumn("date", date_part(YEAR,col('ETL_loaddate'))
   

# COMMAND ----------

#PySpark TimestampType column to DateType -> timestamp to dat
#use cast to convert TimestampType (timestamp string) to DateType using cast function.

from pyspark.sql.functions import expr, to_date

circuit_dt_df = circuit_df.withColumn('race_timestamp',to_date(col('ETL_loaddate')))

# COMMAND ----------

circuit_dt_df.show()

# COMMAND ----------

#circuits_final_df = circuit_df.write.mode("overwrite").partitionBy('country').parquet("dbfs:/FileStore/tables/raw/")

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/FileStore/tables/raw/

# COMMAND ----------

#circuits_final_df = circuit_df.write.mode("overwrite").parquet("dbfs:/FileStore/tables/raw/")

# COMMAND ----------

from pyspark.sql.functions import year
circuit_year_df = circuit_dt_df.withColumn('Year',year(col('race_timestamp')))

# COMMAND ----------

circuit_year_df.display()

# COMMAND ----------

circuit_filter_df = circuit_year_df.filter("circuitId = 11")

# COMMAND ----------

display(circuit_filter_df)

# COMMAND ----------


