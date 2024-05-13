# Databricks notebook source
# MAGIC %run ../_utils

# COMMAND ----------

from pyspark.sql.functions import hour, minute, second, concat, lit, date_format, dayofweek, when
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Camada Silver
# MAGIC
# MAGIC Na camada silver, limpezas e ajustes em dados devem ser aplicados
# MAGIC
# MAGIC Caso seja possível, enriquecer os dados e extrair dados também deve acontecer nessa camada

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - Data ingestion

# COMMAND ----------

df = spark.read.table("bronze.supermarket_sales") # leituira da delta table

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2 - Data Munging
# MAGIC
# MAGIC Processo de limpeza e normalizações necessárias
# MAGIC
# MAGIC Como esses dados já são bem tratados, vamos apenas corrigir o campo "time" para conter a data e verificar a possibilidade de criação de algum outro campo (como campo timestamp ao unir date e time)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### timestamp da venda
# MAGIC
# MAGIC Juntar a data/hora do campo "time" e unir com o campo "date"

# COMMAND ----------


# Extrair a hora, minutos e segundos
df = df.withColumns({"hora": hour(col("time")), 
                     "minuto": minute(col("time")),
                     "segundo": second(col("time"))}) # withColumns eu nao costumo usar, mas vi no linkedin e vim testar rs

# concatenação da data com a hora, minutos e segundos, além dos literais T, : e Z para formar string de timestamp
df = df.withColumn("sale_time", 
                   concat(
                       col("date").cast(StringType()), lit("T"),
                       col("hora").cast(StringType()), lit(":"),
                       col("minuto").cast(StringType()), lit(":"),
                       col("segundo").cast(StringType()), lit("Z")
                   ).cast("timestamp")) # cast para timestamp

# drop de campos intermediarios
df = df.drop("time","hora", "minuto", "segundo")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### mes/ano da venda
# MAGIC
# MAGIC Campo desse tipo ajuda para calcular vendas mensais

# COMMAND ----------

df = df.withColumn("month_year", date_format("date", "MM-yyy"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### taxa por unidade
# MAGIC
# MAGIC um campo apenas para manipulação de dados,
# MAGIC mas será apenas o total de taxa (5%) divididos pela quantidade vendida. tendo assim a taxa unitaria

# COMMAND ----------

df = df.withColumn("total_tax_unity", col("tax_5")/col("quantity"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dia da semana

# COMMAND ----------

df = df.withColumn("day_of_week", dayofweek(col("date")))

# Traduzir os números do dia da semana para o português
df = df.withColumn("day_of_week", 
                   when(df["day_of_week"] == 1, "Domingo")
                   .when(df["day_of_week"] == 2, "Segunda-feira")
                   .when(df["day_of_week"] == 3, "Terça-feira")
                   .when(df["day_of_week"] == 4, "Quarta-feira")
                   .when(df["day_of_week"] == 5, "Quinta-feira")
                   .when(df["day_of_week"] == 6, "Sexta-feira")
                   .when(df["day_of_week"] == 7, "Sábado"))


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### weekend?
# MAGIC
# MAGIC criar um campo para informar se a data é um fim de semana (não faz muito sentido, mas vamos supor que foi uma regra de negócio)

# COMMAND ----------

df = df.withColumn("whekend", 
                   when(col("day_of_week").isin(["Sábado", "Domingo"]), True)
                   .otherwise(False))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC belezura!
# MAGIC
# MAGIC Limpamos um cadim, enriquecemos e transformamos algumas coisas e temos nosso dataset preparado para analises (até preditiva e prescritiva -> mas aí tem que fazer um pouco mais de cleanings e etc)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Saving data

# COMMAND ----------

tb_name = 'silver.supermarket_sales'
target_location = '/FileStore/delta/supermarket_sales/silver'

# COMMAND ----------

save_dataframe(df, "delta", target_location=target_location)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## create delta table
# MAGIC
# MAGIC TODO: implementar UPSERT
# MAGIC
# MAGIC o upsert serve para não precisar reescrever todos os dados, mas aproveitar do Delta para fazer um MERGE, caso um registro antigo tenha uma nova versão e INSERT para os dados que são novos

# COMMAND ----------

create_table(tb_name, target_location)

# COMMAND ----------

# exit para fechar a execução
dbutils.notebook.exit("OK")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from silver.supermarket_sales

# COMMAND ----------


