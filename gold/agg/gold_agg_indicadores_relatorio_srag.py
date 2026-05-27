# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Agregada: Indicadores para Relatório e Agente | SRAG DATASUS
# MAGIC **Tabela:** `certificacao_indicium.poc_srag_datasus.gold_agg_poc_srag_indicadores_relatorio`  
# MAGIC **Grão:** 1 linha por execução — snapshot consolidado de indicadores  
# MAGIC **Estratégia:** OVERWRITE  
# MAGIC **Dependência:** `gold_fact_poc_srag_datasus`
# MAGIC
# MAGIC > Esta tabela é o **ponto de entrada principal do agente** via `tool_sql_metricas_srag`.  
# MAGIC > Contém todos os KPIs prontos para leitura direta, sem necessidade de agregação em tempo de consulta.

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 1] Setup

# COMMAND ----------

import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType
from datetime import date, datetime

RUN_ID     = str(uuid.uuid4())
CATALOGO   = "certificacao_indicium"
SCHEMA     = "poc_srag_datasus"
FULL_FACT  = f"{CATALOGO}.{SCHEMA}.gold_fact_poc_srag_datasus"
FULL_AGG   = f"{CATALOGO}.{SCHEMA}.gold_agg_poc_srag_indicadores_relatorio"

HOJE       = date.today()
HOJE_STR   = HOJE.strftime("%Y-%m-%d")

print(f" Destino  : {FULL_AGG}")
print(f" Hoje     : {HOJE_STR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 2] Carregar Fato e calcular janelas temporais

# COMMAND ----------

df_fact = spark.table(FULL_FACT).filter(F.col("dt_sin_pri").isNotNull())

# Data de referência das janelas = data máxima do próprio dataset.
# Não usar current_date(), pois o arquivo DATASUS é batch/versionado e pode estar defasado.
DATA_REF = df_fact.agg(F.max("dt_sin_pri").alias("max_dt")).first()["max_dt"]
if DATA_REF is None:
    raise ValueError("Não foi possível identificar data máxima do dataset em gold_fact_poc_srag_datasus.")

dt_7d_inicio   = F.date_sub(F.lit(DATA_REF), 6)   # janela inclusiva: DATA_REF e 6 dias anteriores
dt_14d_inicio  = F.date_sub(F.lit(DATA_REF), 13)  # janela anterior: 7 dias imediatamente anteriores
dt_30d_inicio  = F.date_sub(F.lit(DATA_REF), 29)  # últimos 30 dias inclusivos

# ── Casos últimos 7 dias ──────────────────────────────────────────────────────
row_7d = df_fact.filter(
    (F.col("dt_sin_pri") >= dt_7d_inicio) & (F.col("dt_sin_pri") <= F.lit(DATA_REF))
).agg(
    F.sum("flag_caso_srag").alias("casos_7d"),
).first()
casos_7d = int(row_7d["casos_7d"] or 0)

# ── Casos 7 dias anteriores (período de comparação) ───────────────────────────
row_7d_ant = df_fact.filter(
    (F.col("dt_sin_pri") >= dt_14d_inicio) &
    (F.col("dt_sin_pri") <  dt_7d_inicio)
).agg(
    F.sum("flag_caso_srag").alias("casos_7d_ant"),
).first()
casos_7d_ant = int(row_7d_ant["casos_7d_ant"] or 0)

# ── Janela 30 dias ────────────────────────────────────────────────────────────
row_30d = df_fact.filter(
    (F.col("dt_sin_pri") >= dt_30d_inicio) & (F.col("dt_sin_pri") <= F.lit(DATA_REF))
).agg(
    F.sum("flag_caso_srag").alias("casos_30d"),
    F.sum("flag_obito_srag").alias("obitos_30d"),
    F.sum("flag_evolucao_informada").alias("evolucao_30d"),
    F.sum("flag_hospitalizado").alias("hosp_30d"),
    F.sum("flag_uti").alias("uti_30d"),
    F.sum("flag_vacinado_gripe").alias("vac_gripe_30d"),
    F.sum("flag_info_vacina_gripe").alias("info_gripe_30d"),
    F.sum("flag_vacinado_covid").alias("vac_covid_30d"),
    F.sum("flag_info_vacina_covid").alias("info_covid_30d"),
).first()

casos_30d      = int(row_30d["casos_30d"]   or 0)
obitos_30d     = int(row_30d["obitos_30d"]  or 0)
evolucao_30d   = int(row_30d["evolucao_30d"] or 0)
hosp_30d       = int(row_30d["hosp_30d"]    or 0)
uti_30d        = int(row_30d["uti_30d"]     or 0)
vac_gripe_30d  = int(row_30d["vac_gripe_30d"] or 0)
info_gripe_30d = int(row_30d["info_gripe_30d"] or 0)
vac_covid_30d  = int(row_30d["vac_covid_30d"] or 0)
info_covid_30d = int(row_30d["info_covid_30d"] or 0)

# ── Data máxima do dataset ────────────────────────────────────────────────────
data_max = DATA_REF

print(f"✅ Janelas calculadas:")
print(f"   Casos últimos 7 dias    : {casos_7d:,}")
print(f"   Casos 7 dias anteriores : {casos_7d_ant:,}")
print(f"   Casos últimos 30 dias   : {casos_30d:,}")
print(f"   Óbitos últimos 30 dias  : {obitos_30d:,}")
print(f"   Data máxima dataset     : {data_max}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 3] Calcular KPIs consolidados

# COMMAND ----------

# ── Taxa de aumento de casos 7d ───────────────────────────────────────────────
# Fórmula: (casos_7d - casos_7d_ant) / casos_7d_ant
# Se casos_7d_ant = 0 → NULL (não calcular — informar limitação)
if casos_7d_ant > 0:
    taxa_aumento_7d   = round((casos_7d - casos_7d_ant) / casos_7d_ant, 4)
    obs_aumento       = None
else:
    taxa_aumento_7d   = None
    obs_aumento       = "Período anterior sem registros — taxa de aumento não calculável."

# ── Taxa de mortalidade 30d ───────────────────────────────────────────────────
# Denominador: casos com evolução informada (1, 2 ou 3) — exclui ignorados
taxa_mortalidade_30d = (
    round(obitos_30d / evolucao_30d, 4) if evolucao_30d > 0 else None
)

# ── Taxa de uso de UTI 30d ────────────────────────────────────────────────────
# ATENÇÃO: não representa ocupação real de leitos — apenas uso registrado entre SRAG hospitalizados
taxa_uso_uti_30d = (
    round(uti_30d / hosp_30d, 4) if hosp_30d > 0 else None
)

# ── Taxa de vacinação registrada 30d ─────────────────────────────────────────
# ATENÇÃO: não é cobertura vacinal populacional
taxa_vac_gripe_30d = (
    round(vac_gripe_30d / info_gripe_30d, 4) if info_gripe_30d > 0 else None
)
taxa_vac_covid_30d = (
    round(vac_covid_30d / info_covid_30d, 4) if info_covid_30d > 0 else None
)

# ── Defasagem dataset ─────────────────────────────────────────────────────────
if data_max:
    defasagem_dias = (HOJE - data_max).days
    if defasagem_dias <= 7:
        obs_confiabilidade = "Dataset atualizado. Métricas confiáveis."
    elif defasagem_dias <= 30:
        obs_confiabilidade = f"Dataset com {defasagem_dias} dias de defasagem. Tendência recente pode estar subnotificada."
    else:
        obs_confiabilidade = f"Dataset com {defasagem_dias} dias de defasagem. Interpretar métricas recentes com cautela."
else:
    defasagem_dias     = None
    obs_confiabilidade = "Data máxima do dataset não identificada."

print(f"\n📊 KPIs calculados:")
print(f"   taxa_aumento_casos_7d      : {taxa_aumento_7d}")
print(f"   taxa_mortalidade_30d       : {taxa_mortalidade_30d}")
print(f"   taxa_uso_uti_30d           : {taxa_uso_uti_30d}")
print(f"   taxa_vacinacao_gripe_30d   : {taxa_vac_gripe_30d}")
print(f"   taxa_vacinacao_covid_30d   : {taxa_vac_covid_30d}")
print(f"   data_maxima_dataset        : {data_max}")
print(f"   defasagem_dias             : {defasagem_dias}")
print(f"   obs_confiabilidade         : {obs_confiabilidade}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 4] Montar linha de indicadores e gravar

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, TimestampType
)

schema_indicadores = StructType([
    StructField("data_referencia",             DateType(),      True),
    StructField("casos_ultimos_7_dias",        IntegerType(),   True),
    StructField("casos_7_dias_anteriores",     IntegerType(),   True),
    StructField("taxa_aumento_casos_7d",       DoubleType(),    True),
    StructField("casos_ultimos_30_dias",       IntegerType(),   True),
    StructField("obitos_ultimos_30_dias",      IntegerType(),   True),
    StructField("taxa_mortalidade_30d",        DoubleType(),    True),
    StructField("taxa_uso_uti_30d",            DoubleType(),    True),
    StructField("taxa_vacinacao_gripe_30d",    DoubleType(),    True),
    StructField("taxa_vacinacao_covid_30d",    DoubleType(),    True),
    StructField("data_maxima_dataset",         DateType(),      True),
    StructField("defasagem_dias",              IntegerType(),   True),
    StructField("data_execucao",               DateType(),      True),
    StructField("obs_aumento",                 StringType(),    True),
    StructField("observacao_confiabilidade",   StringType(),    True),
    StructField("dh_ingestao_gold",            TimestampType(), True),
])

linha = [(
    DATA_REF,
    casos_7d,
    casos_7d_ant,
    taxa_aumento_7d,
    casos_30d,
    obitos_30d,
    taxa_mortalidade_30d,
    taxa_uso_uti_30d,
    taxa_vac_gripe_30d,
    taxa_vac_covid_30d,
    data_max,
    defasagem_dias,
    HOJE,
    obs_aumento,
    obs_confiabilidade,
    datetime.utcnow(),
)]

df_indicadores = spark.createDataFrame(linha, schema=schema_indicadores)

(
    df_indicadores
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_AGG)
)

print(f"\n✅ {FULL_AGG} gravada com sucesso.")
spark.sql(f"OPTIMIZE {FULL_AGG}")

print(f"\n{'='*60}")
print(f"  ✅ INDICADORES RELATÓRIO CONCLUÍDOS")
print(f"  Tabela  : {FULL_AGG}")
print(f"  Execução: {datetime.utcnow().isoformat()}Z")
print(f"{'='*60}")

dbutils.notebook.exit(f"SUCESSO|run_id={RUN_ID}|agg_indicadores|data_referencia={HOJE_STR}")
