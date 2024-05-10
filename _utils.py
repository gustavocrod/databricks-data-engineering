# Databricks notebook source
!pip install unidecode

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace, trim, round
import unidecode
import re

# COMMAND ----------

def get_diff_between_dates(df, col_1, col_2, delta_type, col_name):
    """
    Função para calcular o tempo entre 2 datas. Primeiramente retorna a data de diferença em segundos e depois converte conforme parametro
    df: dataframe
    col1: tempo maior
    col2: tempo menor
    delta_type: tipo de calculo, por minuto, hora ou dias
    col_name: nome da coluna de retorno
    return: df com a diff col_1 - col_2, conforme delta_type
    """
    if delta_type not in ("minutes", "hours", "days"):
        print("[LOG] delta_type should be minutes, hurs, days")
        return
    else:
        df = df.withColumn(
            "delta_time", col(col_1).cast("long") - col(col_2).cast("long")
        )

        # converter para minutos
        if delta_type == "minutes":
            df = df.withColumn(col_name, round(col("delta_time") / 60, 2))
        # converter para horas
        elif delta_type == "hours":
            df = df.withColumn(col_name, round(col("delta_time") / 3600, 2))
        # converter para dias
        else:
            df = df.withColumn(col_name, round(col("delta_time") / (24 * 3600), 2))
    return df.drop("delta_time")

# COMMAND ----------

def save_dataframe(df, format_mode, target_location, mode="overwrite", table_name=None):
    """
    Ao salvar um DataFrame como tabela Delta, você está escrevendo os dados do DataFrame em um formato Delta diretamente no local especificado no armazenamento. Isso significa que o DataFrame é persistido como uma tabela Delta no local especificado e registrado automaticamente no catálogo Delta do Spark.
    
    save a df into location in parquet or delta (create table) mode
    df: Spark dataframe
    format_mode: should be "delta" or "parquet"
    target_location: target file location to write files :D
    mode: "overwrite" or "append"
    """
    if mode not in ["overwrite", "append"]:
        print(f"[LOG] Not recognized mode {mode}. Should be 'overwrite' or 'append'")

    if format_mode not in ["delta", "parquet"]:
        print(f"[LOG] Not recognized format mode {format_mode}. Should be 'delta' or 'parquet'")

    # Salvar o DataFrame como delta ou parquet
    else:
        print(f"[LOG] Saving {table_name} {format_mode} on {target_location}", end="... ")
        try:
            (df
             .write
             .format(format_mode)
             .mode(mode)
             .option("overwriteSchema", True)
             .save(target_location)
             )
            print("OK!")
        except Exception as e:
            dbutils.notebook.exit(f"[LOG] ERROR writing files: \n{e}")

# COMMAND ----------

def create_table(table_name, target_location):
    """
    Criar uma tabela Delta usando SQL envolve a execução de uma instrução SQL para criar uma nova tabela Delta no catálogo Delta do Spark. Esta operação não envolve escrever dados de um DataFrame, mas sim criar metadados para representar a tabela Delta no catálogo Delta. Depois de criar a tabela Delta, você pode inserir dados nela usando outras operações de ETL.
    """
    from delta.tables import DeltaTable

    print(f"[LOG] Creating delta table {table_name} on {target_location}... ", end="")
    try:
        # aqui estava tendo problema em salvar no dbfs. Tive que utilizar da classe DeltaTable
        delta_table = DeltaTable.forPath(spark, target_location)
        (
            delta_table.alias(table_name)
            .toDF()
            .write.format("delta")
            .option("overwriteSchema", True)
            .mode("overwrite")
            .saveAsTable(table_name)
            
        )
        print("OK!")
    except Exception as e:
        dbutils.notebook.exit(
            f"[LOG] ERROR: Creating table {table_name} on {target_location}\n {e}"
        )

# COMMAND ----------


# alguns metodos que utilizo no dia a dia
def remove_special_characters(s):
  """
  Remove caracteres especiais de uma string
  """
  s = s.replace("\n", "")
  return re.sub(r'[^\w\s]', '', s)

def remove_accents(s):
  """
  remove os acentos de uma string, substiuindo pelo caractere sem acento
  """
  return unidecode.unidecode(s)

def fix_df_column_names(df):
  """
  renomeia colunas, deixando em caixa baixa, removendo espaços em branco do inicio e do fim
  removendo acentos e caracteres especiais
  trocando espaços em branco por underline
  """
  for old_col_name in df.columns:
    new_col_name = remove_accents(old_col_name)
    new_col_name = remove_special_characters(new_col_name)
    new_col_name = new_col_name.lower().strip().replace(" ", "_").replace("", "") # caixa baixa -> sem espaço no final -> espaços por _
    df = df.withColumnRenamed(old_col_name, new_col_name)
  return df
