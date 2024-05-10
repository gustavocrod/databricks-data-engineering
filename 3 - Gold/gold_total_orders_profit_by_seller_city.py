# Databricks notebook source
# MAGIC %run ../_utils

# COMMAND ----------

from pyspark.sql.functions import col, sum, count

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Camada GOLD
# MAGIC
# MAGIC Na camada gold, as limpezas e ajustes já foram feitos, então essa camada é responsável por aplicar regras de negócio, agregações e junções de dados que convirjam para analises.
# MAGIC
# MAGIC Essa é uma tabela sumarizada analítica.
# MAGIC
# MAGIC O objetivo dela é informar o total de venda bruta por cada vendedor (aqui temos cidade)

# COMMAND ----------

tb_name = "olist_gold.total_orders_profit_by_seller_city"
dataset_location = "olist_total_orders_profitprofi_dataset"
target_location = f"dbfs:/FileStore/delta/brazilian_ecommerce/{dataset_location}/gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - Data ingestion
# MAGIC

# COMMAND ----------

df_order_items = spark.read.table("olist_silver.order_items") # temos o seller_id
df_sellers = spark.read.table("olist_silver.sellers")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2 - preparation

# COMMAND ----------

df = df_order_items.join(df_sellers, on=['seller_id'], how="inner")

# COMMAND ----------

df = df.groupBy("seller_city").agg(
    count("order_id").alias("total_orders_by_seller_city"),
    sum("price").alias("total_items_sell_profit"), # total de arrecadação bruta por produto vendido
    sum("freight_value").alias("total_freight_profit") # total de arrecadação bruta por fretes
).withColumn("total_profit", col("total_items_sell_profit") + col("total_freight_profit")) # total arrecadação bruta

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

# COMMAND ----------

create_table(table_name=tb_name, target_location=target_location)

# COMMAND ----------

# exit para fechar a execução
dbutils.notebook.exit("OK")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from olist_gold.total_orders_profit_by_seller_city

# COMMAND ----------


