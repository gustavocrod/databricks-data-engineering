# Databricks notebook source
# MAGIC %run ../_utils

# COMMAND ----------


data = {'table_name': ['customers', 'orders', 'geolocation', 'products', 'order_items', 'sellers', 'order_payments', 'product_category_name_translation', 'order_reviews'],
         'dataset_location': ['olist_customers_dataset', 'olist_orders_dataset', 'olist_geolocation_dataset', 'olist_products_dataset', 'olist_sellers_dataset', 'olist_order_payments_dataset', 'product_category_name_translation', 'olist_order_reviews_dataset'],}


bronze_tables = list(zip(data['table_name'], data['dataset_location']))

# COMMAND ----------

for table_name, dataset_location in bronze_tables:
    print(f"running {table_name}")
    dbutils.notebook.run(path="./generic_bronze_olist", timeout_seconds=3600, arguments={'table_name':table_name, 'dataset_location':dataset_location})

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Problema com versao do databricks

# COMMAND ----------


