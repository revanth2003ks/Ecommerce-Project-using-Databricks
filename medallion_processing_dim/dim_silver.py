# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType,DateType,TimestampType,FloatType

catalog_name = 'ecommerce'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Preprocessing on brz_brands

# COMMAND ----------

df_bronze = spark.table(f'{catalog_name}.bronze.brz_brands')
df_bronze.show()

# COMMAND ----------

# remove extra spaces in the brand names
df_silver= df_bronze.withColumn('brand_name', F.trim(F.col('brand_name')))
df_silver.show(5)

# COMMAND ----------

# replacing non alpha numeric(special charcters) codes with space

df_silver = df_silver.withColumn('brand_code',F.regexp_replace(F.col('brand_code'),r'[^A-Za-z0-9]',''))
df_silver.display() 

# COMMAND ----------

df_silver.select('category_code').distinct().show()

# COMMAND ----------

# Anomiles dictionary

anomalies={
    'GROCERY':'GRCY',
    'BOOKS':'BKS',
    'TOYS':'TOY'
}

df_silver = df_silver.replace(anomalies,subset='category_code')

df_silver.select('category_code').distinct().show()

# COMMAND ----------

# write raw data to the silver layer(catlog: ecommerce, schema: silver, table: slv_brands)
df_silver.write.format('delta')\
    .mode('overwrite')\
        .option('mergeSchema','true')\
            .saveAsTable(f'{catalog_name}.silver.slv_brands')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data processing  on category

# COMMAND ----------

df_bronze = spark.table(f'{catalog_name}.bronze.brz_category')
df_bronze.show()

# COMMAND ----------

df_bronze = df_bronze.withColumn('category_code', F.upper(F.col('category_code')))
df_bronze.show(5)

# COMMAND ----------

# Finding duplicate values
df_duplicates = df_bronze.groupBy('category_code').count().filter(F.col('count')>1)
display(df_duplicates)

#dropping duplicates
df_silver = df_bronze.dropDuplicates(['category_code'])
df_silver.show()


# COMMAND ----------

# write raw data to the silver layer
df_silver.write.format('delta')\
    .mode('overwrite')\
        .option('mergeSchema','true')\
            .saveAsTable(f'{catalog_name}.silver.slv_category')


# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Processing on Products

# COMMAND ----------

#reading raw data
df_bronze = spark.read.table(f'{catalog_name}.bronze.brz_products')

#dropped null columns
df_bronze = df_bronze.drop("file_name","ingest_timestamp")

#get row and column count
row_count, column_count = df_bronze.count(),len(df_bronze.columns)

#printing the result
print(f"row count: {row_count}")
print(f"column count: {column_count}")


# COMMAND ----------

display(df_bronze.limit(5))

# COMMAND ----------

# weight grams has g , removed g
df_silver = df_bronze.withColumn(
    'weight_grams',F.regexp_replace(F.col('weight_grams'),'g','').cast(IntegerType())
)
df_silver.select("weight_grams").show(5,truncate=False)

# COMMAND ----------

# length column has , instead of .
df_silver = df_silver.withColumn(
    'length_cm',F.regexp_replace(F.col('length_cm'),',','.').cast(FloatType())
)
df_silver.select("length_cm").show(5,truncate=False)



# COMMAND ----------

# category and brand_code are in lower case , we need to make it upper case
df_silver = df_silver.withColumn('category_code',F.upper(F.col('category_code')))\
    .withColumn('brand_code',F.upper(F.col('brand_code'))

)


# COMMAND ----------

df_silver.select('material').distinct().show()

# COMMAND ----------

#Fix spelling mistake
df_silver=df_silver.withColumn(
    'material',
    F.when(F.col('material')=='Coton','Cotton')
    .when(F.col('material')=='Alumium','Aluminium')
    .when(F.col("material")=='Ruber','Rubber')
    .otherwise(F.col('material'))
)
df_silver.select("material").distinct().show()

# COMMAND ----------

# negative values in rating_count 
df_silver.filter(F.col('rating_count')<0).select("rating_count").show()

# COMMAND ----------

# converting negative rating_count to positive
df_silver = df_silver.withColumn(
    'rating_count',
    F.when(F.col('rating_count').isNotNull(), F.abs(F.col("rating_count")))
    .otherwise(F.lit(0)) # if it is null then rating_count is 0
)

# COMMAND ----------

display(df_silver.limit(10))

# COMMAND ----------

#with raw data to the siver layer

df_silver.write.format('delta')\
    .mode('overwrite')\
        .option('mergeSchema','true')\
            .saveAsTable(f"{catalog_name}.silver.slv_products")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Customer table data preprocessing

# COMMAND ----------

# reading the raw data from the bronze table
df_bronze = spark.read.table(f'{catalog_name}.bronze.brz_customers')

#get row and column count
row_count = df_bronze.count()
col_count = len(df_bronze.columns)

# print the result
print(f"Row count: {row_count}")
print(f"Column count: {col_count}")


# COMMAND ----------

df_bronze.show(10)

# COMMAND ----------

# handle null values in customer_id
null_count = df_bronze.filter(F.col('customer_id').isNull()).count()
null_count

# COMMAND ----------

# drop row where customer id is null
df_silver = df_bronze.dropna(subset=['customer_id'])

# get row count
row_count = df_silver.count()
print(f"Row count: {row_count}")

# COMMAND ----------

# handling null valuees in phone column
null_count = df_bronze.filter(F.col('phone').isNull()).count()
print(null_count)

# COMMAND ----------

# filling null values with not availabel

df_silver = df_silver.fillna('not available',subset=['phone'])


# COMMAND ----------

df_silver.write.format('delta')\
    .mode('overwrite')\
        .option('mergeSchema','true')\
            .saveAsTable(f"{catalog_name}.silver.slv_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calender/Date

# COMMAND ----------

# read raw data from the bronze table
df_bronze = spark.read.table(f'{catalog_name}.bronze.brz_date')

#get row and column count
row_count = df_bronze.count()
column_count = len(df_bronze.columns)

print(row_count)
print(column_count)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_date

#converting the string column to date type
df_silver = df_bronze.withColumn('date',to_date(df_bronze['date'],'dd-MM-yyyy'))

# COMMAND ----------

print(df_silver.printSchema())

df_silver.show()

# COMMAND ----------

# finding the duplicate values
duplicates = df_silver.groupBy('date').count().filter('count > 1')

print(duplicates.show())

# COMMAND ----------

#remove duplicate rows
df_silver = df_silver.dropDuplicates(['date'])


# COMMAND ----------

# capitalize first letter of each word in day_name
df_silver = df_silver.withColumn('day_name',F.initcap(F.col('day_name')))

df_silver.show(5)

# COMMAND ----------

# convert negative week of the year to positive
df_silver=df_silver.withColumn('week_of_year',F.abs(F.col('week_of_year')))
df_silver.show(5)

# COMMAND ----------


df_silver = df_silver.withColumn('week_of_year',F.concat_ws('-',F.concat_ws('-',F.concat(F.lit('week'),F.col('week_of_year'),F.lit('-'),F.col('year')))))
df_silver = df_silver.withColumn('quarter',F.concat_ws("",F.concat(F.lit("Q"),F.col("quarter"),F.lit("-"),F.col('year'))))
df_silver.show(3)


# COMMAND ----------

# Rename a column
df_silver = df_silver.withColumnRenamed('week_of_year','week')

# COMMAND ----------

df_silver.write.format("delta")\
    .mode('overwrite')\
        .option('mergeSchema','true')\
            .saveAsTable(f'{catalog_name}.silver.slv_calender')