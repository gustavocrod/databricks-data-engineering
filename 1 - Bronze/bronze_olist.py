# Databricks notebook source
# MAGIC %run ../_utils

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Camada bronze
# MAGIC
# MAGIC Na camada bronze, nenhuma limpeza ou regra de negócio devem ser aplicadas aos dados.
# MAGIC
# MAGIC só vamos ler em parquet e salvar em delta.
# MAGIC
# MAGIC Vamos também utilizar da tabela de controle para termos o milestone da ultima execução (aqui nao será utilizado de fato, mas é interessante justamenete para o UPSERT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - Processamento da camada bronze
# MAGIC
# MAGIC Aqui vamos adicionar uma estrutura que permite um laço de repetição.
# MAGIC
# MAGIC O laço será responsável por armazenar os dados e criar tabela delta para cada "entidade" definida no diagrama ER

# COMMAND ----------

data = {
    "table_name": [
        "customers",
        "orders",
        "geolocation",
        "products",
        "order_items",
        "sellers",
        "order_payments",
        "product_category_name_translation",
        "order_reviews",
    ],
    "dataset_location": [
        "olist_customers_dataset",
        "olist_orders_dataset",
        "olist_geolocation_dataset",
        "olist_products_dataset",
        "olist_order_items_dataset",
        "olist_sellers_dataset",
        "olist_order_payments_dataset",
        "product_category_name_translation",
        "olist_order_reviews_dataset",
    ],
}


bronze_tables = list(zip(data["table_name"], data["dataset_location"]))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2 - Persistência
# MAGIC
# MAGIC Estamos pegando os arquivos em parquet (passo apenas didático), salvando os dados em delta e criando as tabelas delta

# COMMAND ----------

for table_name, dataset_location in bronze_tables:
    # read data
    parquet_location = f"/FileStore/parquet/brazilian_ecommerce/{dataset_location}"
    target_location = f"dbfs:/delta/brazilian_ecommerce/{dataset_location}/bronze"

    df = spark.read.parquet(parquet_location)
    tb_name = f"olist_bronze.{table_name}"

    save_dataframe(df, format_mode="delta", table_name=tb_name, target_location=target_location)

    create_table(table_name=tb_name, target_location=target_location)
    
    print()

# COMMAND ----------

dbutils.notebook.exit("OK")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from olist_bronze.order_items

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from olist_bronze.order_items

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from olist_bronze.order_reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from olist_bronze.sellers

# COMMAND ----------


