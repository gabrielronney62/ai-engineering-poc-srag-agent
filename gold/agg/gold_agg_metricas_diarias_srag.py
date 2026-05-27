# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Agregada: Métricas Diárias | SRAG DATASUS
# MAGIC **Tabela:** `certificacao_indicium.poc_srag_datasus.gold_agg_poc_srag_metricas_diarias`  
# MAGIC **Grão:** 1 linha por data de primeiros sintomas (`dt_sin_pri`)  
# MAGIC **Estratégia:** OVERWRITE  
# MAGIC **Dependência:** `gold_fact_poc_srag_datasus`

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 1] Setup

# COMMAND ----------

import uuid
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType

RUN_ID     = str(uuid.uuid4())
CATALOGO   = "certificacao_indicium"
SCHEMA     = "poc_srag_datasus"
FULL_FACT  = f"{CATALOGO}.{SCHEMA}.gold_fact_poc_srag_datasus"
FULL_AGG   = f"{CATALOGO}.{SCHEMA}.gold_agg_poc_srag_metricas_diarias"

print(f" Destino : {FULL_AGG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 2] Agregar por data de primeiros sintomas

# COMMAND ----------

df_fact = spark.table(FULL_FACT).filter(F.col("dt_sin_pri").isNotNull())
DATA_REF = df_fact.agg(F.max("dt_sin_pri").alias("max_dt")).first()["max_dt"]

df_diario = (
    df_fact
    .groupBy("dt_sin_pri")
    .agg(
        F.sum("flag_caso_srag").alias("total_casos"),
        F.sum("flag_obito_srag").alias("total_obitos"),
        F.sum("flag_evolucao_informada").alias("total_casos_com_evolucao"),
        F.sum("flag_hospitalizado").alias("total_hospitalizados"),
        F.sum("flag_uti").alias("total_uti"),
        F.sum("flag_vacinado_gripe").alias("total_vacinados_gripe"),
        F.sum("flag_info_vacina_gripe").alias("total_info_vacina_gripe"),
        F.sum("flag_vacinado_covid").alias("total_vacinados_covid"),
        F.sum("flag_info_vacina_covid").alias("total_info_vacina_covid"),
        F.sum("flag_covid_classificacao_final").alias("total_covid"),
        F.sum("flag_influenza_classificacao_final").alias("total_influenza"),
    )
    .withColumnRenamed("dt_sin_pri", "data_referencia")
    .orderBy("data_referencia")
)

# ── Taxas calculadas ──────────────────────────────────────────────────────────
df_diario = (
    df_diario
    # Taxa de mortalidade = óbitos / casos com evolução informada
    .withColumn("taxa_mortalidade",
        F.when(F.col("total_casos_com_evolucao") > 0,
            F.round(F.col("total_obitos") / F.col("total_casos_com_evolucao"), 4)
        ).otherwise(F.lit(None).cast(DoubleType()))
    )
    # Taxa de uso de UTI = UTI / hospitalizados
    .withColumn("taxa_uso_uti",
        F.when(F.col("total_hospitalizados") > 0,
            F.round(F.col("total_uti") / F.col("total_hospitalizados"), 4)
        ).otherwise(F.lit(None).cast(DoubleType()))
    )
    # Taxa de vacinação gripe registrada (entre casos SRAG — não é cobertura populacional)
    .withColumn("taxa_vacinacao_gripe_registrada",
        F.when(F.col("total_info_vacina_gripe") > 0,
            F.round(F.col("total_vacinados_gripe") / F.col("total_info_vacina_gripe"), 4)
        ).otherwise(F.lit(None).cast(DoubleType()))
    )
    # Taxa de vacinação COVID registrada
    .withColumn("taxa_vacinacao_covid_registrada",
        F.when(F.col("total_info_vacina_covid") > 0,
            F.round(F.col("total_vacinados_covid") / F.col("total_info_vacina_covid"), 4)
        ).otherwise(F.lit(None).cast(DoubleType()))
    )
)

# ── Média Móvel 7 dias ────────────────────────────────────────────────────────
# Window ordenada por data, janela de 7 dias anteriores inclusive o atual
w_mm7 = (
    Window
    .orderBy(F.unix_date(F.col("data_referencia")))
    .rowsBetween(-6, 0)
)

df_diario = df_diario.withColumn(
    "media_movel_7d_casos",
    F.round(F.avg("total_casos").over(w_mm7), 2)
)

df_diario = df_diario.withColumn("dh_ingestao_gold", F.current_timestamp()).withColumn("run_id", F.lit(RUN_ID))

total = df_diario.count()
print(f" Agregação diária construída: {total:,} datas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 3] Amostra dos últimos 30 dias

# COMMAND ----------

print(" Amostra — últimos 30 dias:")
df_diario.filter(
    (F.col("data_referencia") >= F.date_sub(F.lit(DATA_REF), 29)) & (F.col("data_referencia") <= F.lit(DATA_REF))
).select(
    "data_referencia", "total_casos", "total_obitos",
    "taxa_mortalidade", "taxa_uso_uti", "media_movel_7d_casos"
).orderBy(F.col("data_referencia").desc()).show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 4] Gravação — OVERWRITE

# COMMAND ----------

(
    df_diario
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_AGG)
)

print(f"✅ {FULL_AGG} gravada com sucesso.")
spark.sql(f"OPTIMIZE {FULL_AGG} ZORDER BY (data_referencia)")
print(f"✅ OPTIMIZE + Z-ORDER concluídos.")
dbutils.notebook.exit(f"SUCESSO|run_id={RUN_ID}|agg_diaria|linhas={total}")
