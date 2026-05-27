# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Quality: Consolidação de Qualidade | SRAG DATASUS
# MAGIC **Tabela:** `certificacao_indicium.poc_srag_datasus.gold_quality_poc_srag_datasus`  
# MAGIC **Grão:** 1 linha por regra avaliada por execução  
# MAGIC **Estratégia:** APPEND — série histórica de avaliações  
# MAGIC **Dependência:** `bronze_quality_poc_srag_datasus` + `gold_fact_poc_srag_datasus` + `gold_agg_poc_srag_indicadores_relatorio`
# MAGIC
# MAGIC > Esta tabela é consultada pelo agente via `tool_data_quality_srag` antes de  
# MAGIC > emitir qualquer comentário sobre as métricas. Alertas CRÍTICO/ALTO geram  
# MAGIC > avisos obrigatórios no relatório final.

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 1] Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, TimestampType
)
from datetime import datetime
import uuid

CATALOGO          = "certificacao_indicium"
SCHEMA            = "poc_srag_datasus"
FULL_BRZ_QUALITY  = f"{CATALOGO}.{SCHEMA}.bronze_quality_poc_srag_datasus"
FULL_FACT         = f"{CATALOGO}.{SCHEMA}.gold_fact_poc_srag_datasus"
FULL_GOLD_QUALITY = f"{CATALOGO}.{SCHEMA}.gold_quality_poc_srag_datasus"

THRESH_CRITICO = 0.80
THRESH_ALTO    = 0.90

def calcular_severidade(pct: float) -> str:
    if pct < 0.80: return "CRÍTICO"
    if pct < 0.90: return "ALTO"
    if pct < 0.95: return "MÉDIO"
    if pct < 0.99: return "BAIXO"
    return "ACEITÁVEL"

RUN_ID_QUALITY = str(uuid.uuid4())
DH_EXECUCAO = datetime.utcnow()
print(f" Destino : {FULL_GOLD_QUALITY}")
print(f" run_id qualidade: {RUN_ID_QUALITY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 2] Schema da tabela Gold Quality

# COMMAND ----------

SCHEMA_GQ = StructType([
    StructField("run_id",                StringType(),    False),
    StructField("camada",                StringType(),    False),
    StructField("tabela_avaliada",       StringType(),    False),
    StructField("regra_qualidade",       StringType(),    False),
    StructField("dimensao_dmbok",        StringType(),    False),
    StructField("coluna_avaliada",       StringType(),    True),
    StructField("total_registros",       LongType(),      False),
    StructField("registros_validos",     LongType(),      False),
    StructField("registros_invalidos",   LongType(),      False),
    StructField("percentual_validade",   DoubleType(),    False),
    StructField("percentual_invalidade", DoubleType(),    False),
    StructField("severidade",            StringType(),    False),
    StructField("impacto_analitico",     StringType(),    True),
    StructField("recomendacao_tratamento", StringType(),  True),
    StructField("dh_execucao",           TimestampType(), False),
])

registros_gq = []

def reg_gq(camada, tabela, regra, dimensao, coluna,
           total, validos, impacto, recomendacao):
    invalidos    = total - validos
    pct_val      = round(validos / total, 6) if total > 0 else 0.0
    pct_inv      = round(invalidos / total, 6) if total > 0 else 0.0
    return {
        "run_id":                  RUN_ID_QUALITY,
        "camada":                  camada,
        "tabela_avaliada":         tabela,
        "regra_qualidade":         regra,
        "dimensao_dmbok":          dimensao,
        "coluna_avaliada":         coluna,
        "total_registros":         total,
        "registros_validos":       validos,
        "registros_invalidos":     invalidos,
        "percentual_validade":     pct_val,
        "percentual_invalidade":   pct_inv,
        "severidade":              calcular_severidade(pct_val),
        "impacto_analitico":       impacto,
        "recomendacao_tratamento": recomendacao,
        "dh_execucao":             DH_EXECUCAO,
    }

print("✅ Schema e função auxiliar definidos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 3] Herdar alertas críticos da Bronze Quality

# COMMAND ----------

print(" Herdando alertas da Bronze Quality...")

# Ler avaliação mais recente da Bronze Quality
dh_max_brz = (
    spark.table(FULL_BRZ_QUALITY)
    .agg(F.max("dh_execucao").alias("max_dh"))
    .first()["max_dh"]
)

df_brz_quality = (
    spark.table(FULL_BRZ_QUALITY)
    .filter(F.col("dh_execucao") == F.lit(dh_max_brz))
    .filter(F.col("severidade").isin("CRÍTICO", "ALTO"))
)

# Converter para registros Gold Quality com campos adicionais
IMPACTOS_BRONZE = {
    "classi_fin": "Afeta TODAS as taxas de classificação (COVID, Influenza, SRAG não especificado).",
    "evolucao":   "Afeta diretamente a taxa de mortalidade. Casos sem evolução são excluídos do denominador.",
    "uti":        "Afeta taxa de uso de UTI entre hospitalizados.",
    "vacina":     "Afeta taxa de vacinação contra gripe registrada.",
    "vacina_cov": "Afeta taxa de vacinação COVID registrada.",
    "nu_notific": "Nulos impedem deduplicação — duplicatas podem inflar contagem total de casos.",
    "dt_sin_pri": "Nulos excluem registros das séries temporais e das agregadas diária e mensal.",
}

RECOMENDACOES_BRONZE = {
    "classi_fin": "Investigar registros sem classificação. Avaliar se são casos ainda em investigação.",
    "evolucao":   "Considerar janela de 30 dias para casos recentes (atraso de digitação esperado).",
    "uti":        "Verificar se campos UTI=NULL representam ausência de internação ou não preenchimento.",
    "vacina":     "Campos NULL tratados como 'informação ausente' — excluídos do denominador da taxa.",
    "vacina_cov": "Idem vacina. Verificar integração com Base Nacional de Vacinação.",
    "nu_notific": "Registros sem nu_notific usam chave técnica alternativa na Silver.",
    "dt_sin_pri": "Registros sem dt_sin_pri são excluídos de todas as séries temporais Gold.",
}

for row in df_brz_quality.collect():
    col = row["coluna_avaliada"]
    observacao_bronze = row["observacao"] if "observacao" in row.asDict() else "Ver Bronze Quality."
    registros_gq.append(reg_gq(
        camada     = "Bronze",
        tabela     = row["tabela_avaliada"],
        regra      = f"[Herdado Bronze] {row['regra_qualidade']}",
        dimensao   = row["dimensao_dmbok"],
        coluna     = col,
        total      = row["total_registros"],
        validos    = row["registros_validos"],
        impacto    = IMPACTOS_BRONZE.get(col, observacao_bronze),
        recomendacao = RECOMENDACOES_BRONZE.get(col, "Investigar registros inválidos na Bronze."),
    ))

print(f"   Alertas herdados da Bronze: {len(registros_gq)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 4] Validações próprias da Gold

# COMMAND ----------

print(" Validações Gold:")

df_fact = spark.table(FULL_FACT)
total_fact = df_fact.count()

# ── G1: Completude de sk_tempo ────────────────────────────────────────────────
validos_tempo = df_fact.filter(F.col("sk_tempo").isNotNull()).count()
registros_gq.append(reg_gq(
    camada="Gold", tabela=FULL_FACT,
    regra="Completude - sk_tempo (dt_sin_pri não nulo)",
    dimensao="Completude", coluna="sk_tempo",
    total=total_fact, validos=validos_tempo,
    impacto="Casos sem sk_tempo não aparecem nas séries temporais do dashboard.",
    recomendacao="Registros sem dt_sin_pri devem ser investigados na origem (SIVEP-Gripe).",
))

# ── G2: Completude de sk_classificacao ───────────────────────────────────────
validos_classi = df_fact.filter(F.col("sk_classificacao").isNotNull()).count()
registros_gq.append(reg_gq(
    camada="Gold", tabela=FULL_FACT,
    regra="Completude - sk_classificacao",
    dimensao="Completude", coluna="sk_classificacao",
    total=total_fact, validos=validos_classi,
    impacto="Casos sem classificação não contribuem para taxas por agente etiológico.",
    recomendacao="Verificar casos em investigação no SIVEP-Gripe.",
))

# ── G3: Validade — flag_obito_srag ────────────────────────────────────────────
taxa_obito = df_fact.agg(
    F.sum("flag_obito_srag").alias("obitos"),
    F.sum("flag_evolucao_informada").alias("com_evolucao")
).first()
obitos     = int(taxa_obito["obitos"] or 0)
evolucao   = int(taxa_obito["com_evolucao"] or 0)
taxa_mort  = round(obitos / evolucao, 4) if evolucao > 0 else None

# Taxa de mortalidade razoável para SRAG = entre 0 e 0.60
taxa_valida = 1 if (taxa_mort is not None and 0 <= taxa_mort <= 0.60) else 0
registros_gq.append(reg_gq(
    camada="Gold", tabela=FULL_FACT,
    regra=f"Validade - taxa_mortalidade ({taxa_mort})",
    dimensao="Acurácia", coluna="flag_obito_srag / flag_evolucao_informada",
    total=1, validos=taxa_valida,
    impacto=f"Taxa de mortalidade calculada: {taxa_mort}. Esperado entre 0 e 0.60 para SRAG.",
    recomendacao="Se > 0.60, investigar subnotificação de curas ou erro na coluna evolucao.",
))

# ── G4: Validade — taxa_uso_uti ───────────────────────────────────────────────
uti_row    = df_fact.agg(
    F.sum("flag_uti").alias("uti"),
    F.sum("flag_hospitalizado").alias("hosp")
).first()
uti_total  = int(uti_row["uti"] or 0)
hosp_total = int(uti_row["hosp"] or 0)
taxa_uti   = round(uti_total / hosp_total, 4) if hosp_total > 0 else None
uti_valida = 1 if (taxa_uti is not None and 0 <= taxa_uti <= 1.0) else 0

registros_gq.append(reg_gq(
    camada="Gold", tabela=FULL_FACT,
    regra=f"Validade - taxa_uso_uti ({taxa_uti})",
    dimensao="Acurácia", coluna="flag_uti / flag_hospitalizado",
    total=1, validos=uti_valida,
    impacto=f"Taxa de uso de UTI: {taxa_uti}. Deve estar entre 0 e 1.",
    recomendacao="Não representa ocupação real de leitos — apenas uso registrado entre SRAG hospitalizados.",
))

# ── G5: Atualidade — data máxima do dataset ───────────────────────────────────
from datetime import date
data_max   = df_fact.agg(F.max("dt_sin_pri")).first()[0]
hoje       = date.today()
defasagem  = (hoje - data_max).days if data_max else 999
atu_valida = 1 if defasagem <= 30 else 0

registros_gq.append(reg_gq(
    camada="Gold", tabela=FULL_FACT,
    regra=f"Atualidade - data máxima dt_sin_pri ({data_max}, defasagem {defasagem}d)",
    dimensao="Atualidade", coluna="dt_sin_pri",
    total=1, validos=atu_valida,
    impacto=f"Defasagem de {defasagem} dias. Métricas recentes podem estar subnotificadas.",
    recomendacao="Atraso de digitação no SIVEP-Gripe é esperado (7-30 dias). Comunicar no relatório.",
))

print(f"   Validações Gold geradas: {len(registros_gq) - len([r for r in registros_gq if 'Herdado' in r['regra_qualidade']])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 5] Gravar Gold Quality — APPEND

# COMMAND ----------

df_gq = spark.createDataFrame(registros_gq, schema=SCHEMA_GQ)

(
    df_gq
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(FULL_GOLD_QUALITY)
)

print(f"✅ {FULL_GOLD_QUALITY} atualizada. {len(registros_gq)} regras gravadas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 6] Resumo para o Agente

# COMMAND ----------

total_critico = sum(1 for r in registros_gq if r["severidade"] == "CRÍTICO")
total_alto    = sum(1 for r in registros_gq if r["severidade"] == "ALTO")

print(f"\n{'='*60}")
print(f"  📊 GOLD QUALITY — RESUMO PARA O AGENTE")
print(f"  Total de regras avaliadas : {len(registros_gq)}")
print(f"  Alertas CRÍTICO           : {total_critico}")
print(f"  Alertas ALTO              : {total_alto}")
if total_critico + total_alto > 0:
    print(f"\n  ⚠️  O agente deve incluir aviso de qualidade no relatório.")
else:
    print(f"\n  ✅ Sem alertas críticos/altos — métricas confiáveis.")
print(f"{'='*60}")

spark.sql(f"OPTIMIZE {FULL_GOLD_QUALITY}")
dbutils.notebook.exit(
    f"SUCESSO|run_id_quality={RUN_ID_QUALITY}|gold_quality|regras={len(registros_gq)}"
    f"|criticos={total_critico}|altos={total_alto}"
)
