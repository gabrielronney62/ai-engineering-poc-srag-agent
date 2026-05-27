# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Dimensão Classificação | SRAG DATASUS
# MAGIC **Tabela:** `certificacao_indicium.poc_srag_datasus.gold_dim_classificacao_poc_srag_datasus`  
# MAGIC **Grão:** 1 linha por combinação única de classificação final + critério + evolução  
# MAGIC **Estratégia:** OVERWRITE  
# MAGIC **Dependência:** `silver_poc_srag_datasus`

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 1] Setup

# COMMAND ----------

import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

RUN_ID      = str(uuid.uuid4())
CATALOGO    = "certificacao_indicium"
SCHEMA      = "poc_srag_datasus"
FULL_SILVER = f"{CATALOGO}.{SCHEMA}.silver_poc_srag_datasus"
FULL_DIM    = f"{CATALOGO}.{SCHEMA}.gold_dim_classificacao_poc_srag_datasus"

print(f" Destino : {FULL_DIM}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 2] Decodificadores de domínio

# COMMAND ----------

classi_desc = (
    F.when(F.col("classi_fin") == 1, F.lit("SRAG por Influenza"))
     .when(F.col("classi_fin") == 2, F.lit("SRAG por outro vírus respiratório"))
     .when(F.col("classi_fin") == 3, F.lit("SRAG por outro agente etiológico"))
     .when(F.col("classi_fin") == 4, F.lit("SRAG não especificado"))
     .when(F.col("classi_fin") == 5, F.lit("SRAG por COVID-19"))
     .otherwise(F.lit("Não informado"))
)

criterio_desc = (
    F.when(F.col("criterio") == 1, F.lit("Laboratorial"))
     .when(F.col("criterio") == 2, F.lit("Clínico Epidemiológico"))
     .when(F.col("criterio") == 3, F.lit("Clínico"))
     .when(F.col("criterio") == 4, F.lit("Clínico Imagem"))
     .otherwise(F.lit("Não informado"))
)

evolucao_desc = (
    F.when(F.col("evolucao") == 1, F.lit("Cura"))
     .when(F.col("evolucao") == 2, F.lit("Óbito"))
     .when(F.col("evolucao") == 3, F.lit("Óbito por outras causas"))
     .when(F.col("evolucao") == 9, F.lit("Ignorado"))
     .otherwise(F.lit("Não informado"))
)

print(" Decodificadores definidos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 3] Extrair combinações únicas e construir dimensão

# COMMAND ----------

df_silver = (
    spark.table(FULL_SILVER)
    .filter(F.col("registro_mais_atual") == True)
)

df_dim_classificacao = (
    df_silver
    .select(
        F.coalesce(F.col("classi_fin").cast(StringType()), F.lit("n/a")).alias("classi_str"),
        F.col("classi_fin"),
        F.coalesce(F.col("criterio").cast(StringType()), F.lit("n/a")).alias("criterio_str"),
        F.col("criterio"),
        F.coalesce(F.col("evolucao").cast(StringType()), F.lit("n/a")).alias("evolucao_str"),
        F.col("evolucao"),
    )
    .distinct()
    .withColumn(
        "sk_classificacao",
        F.sha2(
            F.concat_ws("||",
                F.col("classi_str"),
                F.col("criterio_str"),
                F.col("evolucao_str"),
            ), 256
        )
    )
    .withColumn("descricao_classificacao_final", classi_desc)
    .withColumn("descricao_criterio",            criterio_desc)
    .withColumn("descricao_evolucao",            evolucao_desc)
    .withColumn("dh_ingestao_gold",              F.current_timestamp())
    .withColumn("run_id", F.lit(RUN_ID))
    .select(
        "sk_classificacao",
        "classi_fin", "descricao_classificacao_final",
        "criterio",   "descricao_criterio",
        "evolucao",   "descricao_evolucao",
        "dh_ingestao_gold",
    )
    .dropDuplicates(["sk_classificacao"])
)

total = df_dim_classificacao.count()
print(f" Dimensão construída: {total:,} linhas | {len(df_dim_classificacao.columns)} colunas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 4] Gravação — OVERWRITE

# COMMAND ----------

(
    df_dim_classificacao
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_DIM)
)

print(f" {FULL_DIM} gravada com sucesso.")
spark.sql(f"OPTIMIZE {FULL_DIM}")
dbutils.notebook.exit(f"SUCESSO|run_id={RUN_ID}|dim_classificacao|linhas={total}")
