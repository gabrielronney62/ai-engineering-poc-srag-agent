# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Dimensão Localidade | SRAG DATASUS
# MAGIC **Tabela:** `certificacao_indicium.poc_srag_datasus.gold_dim_localidade_poc_srag_datasus`  
# MAGIC **Grão:** 1 linha por combinação única UF + município de residência  
# MAGIC **Estratégia:** OVERWRITE  
# MAGIC **Dependência:** `silver_poc_srag_datasus`

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 1] Setup

# COMMAND ----------

from pyspark.sql import functions as F
import uuid
from datetime import datetime

RUN_ID      = str(uuid.uuid4())
CATALOGO    = "certificacao_indicium"
SCHEMA      = "poc_srag_datasus"
FULL_SILVER = f"{CATALOGO}.{SCHEMA}.silver_poc_srag_datasus"
FULL_DIM    = f"{CATALOGO}.{SCHEMA}.gold_dim_localidade_poc_srag_datasus"

print(f" Destino : {FULL_DIM}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 2] Extrair combinações únicas de localidade

# COMMAND ----------

df_silver = (
    spark.table(FULL_SILVER)
    .filter(F.col("registro_mais_atual") == True)
)

# Combinações únicas de residência + internação
df_loc = (
    df_silver
    .select(
        F.coalesce(F.col("sg_uf_residencia"),  F.lit("n/a")).alias("sg_uf_residencia"),
        F.coalesce(F.col("co_mun_res"), F.lit("n/a")).alias("co_mun_residencia"),
        F.col("id_mn_resi"),
        F.col("sg_uf_inte"),
        F.col("co_mu_inte"),
        F.col("id_mn_inte"),
    )
    .distinct()
)

total = df_loc.count()
print(f" Combinações únicas de localidade: {total:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 3] Construir dimensão Localidade

# COMMAND ----------

df_dim_localidade = (
    df_loc
    .withColumn(
        "sk_localidade",
        F.sha2(
            F.concat_ws("||",
                F.col("sg_uf_residencia"),
                F.col("co_mun_residencia")
            ), 256
        )
    )
    .withColumn("dh_ingestao_gold", F.current_timestamp())
    .withColumn("run_id", F.lit(RUN_ID))
    .select(
        "sk_localidade",
        F.col("sg_uf_residencia").alias("sg_uf"),
        "co_mun_residencia",
        "id_mn_resi",
        "sg_uf_inte",
        "co_mu_inte",
        "id_mn_inte",
        "dh_ingestao_gold",
    )
    # Deduplicar SK em caso de colisão improvável
    .dropDuplicates(["sk_localidade"])
)

print(f" Dimensão construída: {df_dim_localidade.count():,} linhas | {len(df_dim_localidade.columns)} colunas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 4] Gravação — OVERWRITE

# COMMAND ----------

(
    df_dim_localidade
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_DIM)
)

print(f" {FULL_DIM} gravada com sucesso.")
spark.sql(f"OPTIMIZE {FULL_DIM} ZORDER BY (sg_uf)")
print(f" OPTIMIZE + Z-ORDER concluídos.")
dbutils.notebook.exit(f"SUCESSO|run_id={RUN_ID}|dim_localidade|linhas={df_dim_localidade.count()}")
