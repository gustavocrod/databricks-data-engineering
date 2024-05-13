# Databricks notebook source
# MAGIC %run ../../_utils

# COMMAND ----------

from pyspark.sql.functions import count, sum, when

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Sales by month
# MAGIC
# MAGIC Aqui nessa tabela iremos agrupar as vendas por mês
# MAGIC
# MAGIC como é uma tabela gold e não diretamente um valor agregado (ainda antes de um datamart), iremos agrupar e permitir que calculos sejam feitos a partir dos dados de output da camada gold.
# MAGIC O que isso significa?
# MAGIC
# MAGIC Significa que teremos dados agrupados mas ainda segmentados, permitindo que o visualizador otimize a análise após receber o dado já tratado (ideia da camada gold)

# COMMAND ----------

!pip install pandas_market_calendars

# COMMAND ----------

tb_name = 'gold.supermarket_sales'
target_location = '/FilesStore/delta/supermarket_sales/gold'

# COMMAND ----------

def is_business_day(date):
    # calcula dia util
    calendar = get_calendar('BMF') # calendario brasileiro só porque nao encontrei o calendario da 
    return calendar.valid_days(start_date=date, end_date=date).shape[0] > 0

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Data ingestion

# COMMAND ----------

df = spark.sql("select * from silver.supermarket_sales") # outra maneira de ler
# df = spark.read.table("silver.supermarket_sales")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### grouping

# COMMAND ----------



# COMMAND ----------

display(df.groupBy("month_year", "city", "branch", "customer_type", "gender", "product_line")
        .agg(
            count("invoice_id").alias("total_sales"),
            sum("quantity").alias("total_products_sales"),
            sum("gross_income").alias("total_gross_income")
            )
 )

# COMMAND ----------


