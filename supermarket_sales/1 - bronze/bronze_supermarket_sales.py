# Databricks notebook source
# MAGIC %run ../_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - leitura dos dados

# COMMAND ----------

parquet_location = "/FileStore/parquet/supermarket_sales"

df = spark.read.parquet(parquet_location)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### teste de armazenamento em delta

# COMMAND ----------

save_dataframe(df, format_mode="delta", target_location='/FilesStore/delta/supermarket_sales')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Camada bronze
# MAGIC
# MAGIC Na camada bronze, nenhuma limpeza ou regra de negócio devem ser aplicadas aos dados.
# MAGIC
# MAGIC Porém aqui temos dados que demandam de um pequeno tratamento: nome dos campos.
# MAGIC
# MAGIC Para armazenarmos em formato delta, precisaremos tratar o nome dos campos. (outros tratamentos devem acontecer na camada subsequente)
# MAGIC
# MAGIC Para essa limpeza dos dados, utilizamos do método ``fix_df_column_names`` definido em ``_utils``

# COMMAND ----------

df = fix_df_column_names(df)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Save dataframe

# COMMAND ----------

tb_name = 'bronze.supermarket_sales'
target_location = '/FilesStore/delta/supermarket_sales/bronze'

# COMMAND ----------

save_dataframe(df, format_mode="delta", target_location=target_location)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Criar tabela delta

# COMMAND ----------

create_table(tb_name, target_location)

# COMMAND ----------

# para sair do notebook
# tudo que rodar após isso será ignorado quando o notebook for executado via workflow
dbutils.notebook.exit("OK")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze.supermarket_sales

# COMMAND ----------


