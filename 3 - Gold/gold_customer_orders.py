# Databricks notebook source
# MAGIC %run ../_utils

# COMMAND ----------

from pyspark.sql.functions import col, count

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Customers orders
# MAGIC
# MAGIC Essa é uma tabela de sumarização.
# MAGIC
# MAGIC O objetivo dela é responder sobre as compras dos clientes.
# MAGIC
# MAGIC Conseguiríamos responder questões como:
# MAGIC  - Quantas vendas ocorreram por estado
# MAGIC  - poderiamos ver as vendas por mes e ano
# MAGIC  - poderiamos ver dados sobre valores das vendas
# MAGIC  - dados sobre as entregas, como a relação do dia da compra e atraso na entrega

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


