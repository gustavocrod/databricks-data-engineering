# Databricks notebook source
# MAGIC %run ../_utils

# COMMAND ----------

from pyspark.sql.functions import col, count

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Camada GOLD
# MAGIC
# MAGIC Na camada gold, as limpezas e ajustes já foram feitos, então essa camada é responsável por aplicar regras de negócio, agregações e junções de dados que convirjam para analises.
# MAGIC
# MAGIC Essa é uma tabela de sumarização.
# MAGIC
# MAGIC O objetivo dela é permitir analises

# COMMAND ----------

tb_name = "olist_gold.customer_orders"
dataset_location = "olist_customer_ordersdataset"
target_location = f"dbfs:/FileStore/delta/brazilian_ecommerce/{dataset_location}/gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - Data ingestion
# MAGIC

# COMMAND ----------

df_orders = spark.read.table("olist_gold.orders") # leituira da delta table central, orders (ja da propria gold)
df_customers = spark.sql("select * from olist_silver.customers") # leitura da delta table, outra maneira

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2 - preparation

# COMMAND ----------

display(df_orders)

# COMMAND ----------

display(df_customers)

# COMMAND ----------

df = df_orders.join(df_customers, on="customer_id", how="left")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Saving data

# COMMAND ----------

save_dataframe(df, format_mode="delta", table_name=tb_name, target_location=target_location)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## create delta table
# MAGIC

# COMMAND ----------

create_table(table_name=tb_name, target_location=target_location)

# COMMAND ----------

# exit para fechar a execução
dbutils.notebook.exit("OK")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from olist_gold.orders

# COMMAND ----------


