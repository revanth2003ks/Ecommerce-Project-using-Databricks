# Databricks notebook source
# MAGIC %md
# MAGIC ## catalog setup

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists ecommerce;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ecommerce;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists ecommerce.bronze;
# MAGIC create schema if not exists ecommerce.silver;
# MAGIC create schema if not exists ecommerce.gold;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES FROM ecommerce;

# COMMAND ----------


# to delete database
# DROP CATALOG IF EXISTS ECOMMRCE CASCADE;

# COMMAND ----------

