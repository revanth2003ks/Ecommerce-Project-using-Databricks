# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType,IntegerType,DateType,TimestampType,FloatType
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transferring raw data to Bronze layer with defined schema
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Brands

# COMMAND ----------

catalog_name = 'ecommerce'

#Define schema for the data file
brand_schema = StructType((
    StructField('brand_code',StringType(),False),
    StructField('brand_name',StringType(),True),
    StructField('category_code',StringType(),True)
))

# COMMAND ----------

raw_data_path = '/Volumes/ecommerce/source_data/raw/brands/*.csv'

# COMMAND ----------

df = spark.read.option('header','true').option('delimeter',',').schema(brand_schema).csv(raw_data_path)

# adding metadata columns
df = df.withColumn("_source_file",F.col('_metadata.file_path'))

df.display(5)

# COMMAND ----------

df.write.format('delta')\
    .mode("overwrite")\
        .option("mergeSchema", "true")\
        .saveAsTable(f"{catalog_name}.bronze.brz_brands")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Category

# COMMAND ----------

catalog_name = 'ecommerce'

category_schema = StructType((
    StructField('category_code',StringType(),False),
    StructField('category_name',StringType(),True)
))

#Load data using the schema defined

raw_data_path = "/Volumes/ecommerce/source_data/raw/category/*.csv"

df_raw = spark.read.option('header','true').option('delimeter',',').schema(category_schema).csv(raw_data_path)

# adding metadata columns
df_raw = df_raw.withColumn("_source_file",F.col('_metadata.file_path'))\
    .withColumn('_ingested_at',F.current_timestamp())

# write raw data to the bronze layer 
df_raw.write.format('delta')\
    .mode('overwrite')\
        .option("mergeSchema", "true")\
        .saveAsTable(f"{catalog_name}.bronze.brz_category")

df_raw.display(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Products

# COMMAND ----------

catalog_name = 'ecommerce'

products_schema = StructType((
    StructField('product_id',StringType(),False),
    StructField('sku',StringType(),True),
    StructField('category_code',StringType(),True),
    StructField('brand_code',StringType(),True),
    StructField('color',StringType(),True),
    StructField('size',StringType(),True),
    StructField('material',StringType(),True),
    StructField('weight_grams',StringType(),True),
    StructField('length_cm',StringType(),True),
    StructField('height_cm',FloatType(),True),
    StructField('width_cm',FloatType(),True),
    StructField('rating_count',IntegerType(),True),
    StructField('_source_file',StringType(),False),
    StructField('_ingested_at',TimestampType(),False)


))

# Load data using the schema defined

raw_data_path = "/Volumes/ecommerce/source_data/raw/products/*.csv"

df_raw = spark.read.option('header','true').option('delimeter',',').schema(products_schema).csv(raw_data_path)\
    .withColumn('_source_file',F.col('_metadata.file_path'))\
    .withColumn('_ingested_at',F.current_timestamp())

# write raw data to the bronze layer 
df_raw.write.format('delta')\
    .mode('overwrite')\
        .option("mergeSchema", "true")\
        .saveAsTable(f"{catalog_name}.bronze.brz_products")

df_raw.display(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers

# COMMAND ----------

customers_schema = StructType((
    StructField('customer_id',StringType(),False),
    StructField('phone',StringType(),True),
    StructField('country_code',StringType(),True),
    StructField('country',StringType(),True),
    StructField('state',StringType(),True)
))

#Load data using the schema defined

raw_data_path = "/Volumes/ecommerce/source_data/raw/customers/*.csv"

df_raw = spark.read.option('header','true').option('delimeter',',').schema(customers_schema).csv(raw_data_path)\
    .withColumn('_source_file',F.col('_metadata.file_path'))\
    .withColumn('_ingested_at',F.current_timestamp())

# write raw data to the bronze layer 
df_raw.write.format('delta')\
    .mode('overwrite')\
        .option("mergeSchema", "true")\
        .saveAsTable(f"{catalog_name}.bronze.brz_customers")

df_raw.display(5)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Orders

# COMMAND ----------

date_scchema = StructType((
    StructField('date',StringType(),False),
    StructField('year',StringType(),True),
    StructField('day_name',StringType(),True),
    StructField('quarter',IntegerType(),True),
    StructField('week_of_year',IntegerType(),True)))

#Load data using the schema defined

raw_data_path = "/Volumes/ecommerce/source_data/raw/date/*.csv"

df_raw = spark.read.option('header','true').option('delimeter',',').schema(date_scchema).csv(raw_data_path)\
    .withColumn('_source_file',F.col('_metadata.file_path'))\
    .withColumn('_ingested_at',F.current_timestamp())

# write raw data to the bronze layer 
df_raw.write.format('delta')\
    .mode('overwrite')\
        .option("mergeSchema", "true")\
        .saveAsTable(f"{catalog_name}.bronze.brz_date")




# COMMAND ----------

# MAGIC %md
# MAGIC #### Products

# COMMAND ----------

