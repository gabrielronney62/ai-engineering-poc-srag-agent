# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze Quality: Profiling DMBOK — SRAG DATASUS
# MAGIC
# MAGIC **PoC:** HealthCare Indicium — Monitoramento de SRAG  
# MAGIC **Camada:** Bronze Quality  
# MAGIC **Estratégia de escrita:** APPEND (série histórica de execuções)  
# MAGIC **Fonte:** `certificacao_indicium.poc_srag_datasus.bronze_poc_srag_datasus`  
# MAGIC **Tabela destino:** `certificacao_indicium.poc_srag_datasus.bronze_quality_poc_srag_datasus`
# MAGIC
# MAGIC ---
# MAGIC ### O que este notebook faz?
# MAGIC Executa **profiling inicial** do dado bruto antes da transformação Silver.  
# MAGIC Mede qualidade em 5 dimensões DMBOK:
# MAGIC 1. **Completude** — campos obrigatórios e essenciais preenchidos
# MAGIC 2. **Validade de domínio** — valores dentro dos domínios esperados
# MAGIC 3. **Consistência temporal** — relações entre datas coerentes
# MAGIC 4. **Unicidade** — duplicatas por chave natural
# MAGIC 5. **Atualidade** — defasagem entre data máxima e execução
# MAGIC
# MAGIC > ⚠️ Esta tabela **não corrige** dados. Apenas mede e registra.

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 1] Setup e Dependências

# COMMAND ----------

import uuid
import re
from datetime import datetime, date
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, DoubleType, TimestampType
)

print("    Bibliotecas carregadas.")
print(f"   Spark version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 2] Parâmetros e Variáveis de Controle

# COMMAND ----------

# ── Identificação da Execução ──────────────────────────────────────────────────
RUN_ID      = str(uuid.uuid4())
DH_EXECUCAO = datetime.utcnow()

# ── Origem / Destino ───────────────────────────────────────────────────────────
CATALOGO          = "certificacao_indicium"
SCHEMA            = "poc_srag_datasus"
TABELA_BRONZE     = "bronze_poc_srag_datasus"
TABELA_QUALITY    = "bronze_quality_poc_srag_datasus"

FULL_BRONZE       = f"{CATALOGO}.{SCHEMA}.{TABELA_BRONZE}"
FULL_QUALITY      = f"{CATALOGO}.{SCHEMA}.{TABELA_QUALITY}"

# ── Thresholds de Severidade ───────────────────────────────────────────────────
THRESH_CRITICO = 0.80
THRESH_ALTO    = 0.90
THRESH_MEDIO   = 0.95
THRESH_BAIXO   = 0.99

print(f" run_id           : {RUN_ID}")
print(f" Execução         : {DH_EXECUCAO.isoformat()}Z")
print(f" Bronze           : {FULL_BRONZE}")
print(f" Quality destino  : {FULL_QUALITY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 3] Schema da Tabela de Qualidade e Funções Base

# COMMAND ----------

SCHEMA_QUALITY = StructType([
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
    StructField("observacao",            StringType(),    True),
    StructField("dh_execucao",           TimestampType(), False),
])


def calcular_severidade(pct_validade: float) -> str:
    if pct_validade < THRESH_CRITICO:
        return "CRÍTICO"
    elif pct_validade < THRESH_ALTO:
        return "ALTO"
    elif pct_validade < THRESH_MEDIO:
        return "MÉDIO"
    elif pct_validade < THRESH_BAIXO:
        return "BAIXO"
    return "ACEITÁVEL"


def criar_registro_qualidade(
    regra: str,
    dimensao: str,
    coluna: str,
    total: int,
    validos: int,
    observacao: str = None
) -> dict:
    invalidos      = total - validos
    pct_validade   = round(validos / total, 6)   if total > 0 else 0.0
    pct_invalidade = round(invalidos / total, 6) if total > 0 else 0.0
    return {
        "run_id":                RUN_ID,
        "camada":                "Bronze",
        "tabela_avaliada":       FULL_BRONZE,
        "regra_qualidade":       regra,
        "dimensao_dmbok":        dimensao,
        "coluna_avaliada":       coluna,
        "total_registros":       total,
        "registros_validos":     validos,
        "registros_invalidos":   invalidos,
        "percentual_validade":   pct_validade,
        "percentual_invalidade": pct_invalidade,
        "severidade":            calcular_severidade(pct_validade),
        "observacao":            observacao,
        "dh_execucao":           datetime.utcnow(),
    }


print(" Schema e funções base definidos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 4] Leitura da Bronze — apenas o run mais recente

# COMMAND ----------

# Ler TODOS os dados do run mais recente para avaliação isolada
df_bronze = spark.table(FULL_BRONZE)

# Identificar run mais recente da Bronze via run_id + dh_ingestao_bronze
run_mais_recente = (
    df_bronze
    .select("run_id", "dh_ingestao_bronze")
    .orderBy(F.col("dh_ingestao_bronze").desc())
    .first()["run_id"]
)

df_eval = df_bronze.filter(F.col("run_id") == run_mais_recente)
TOTAL_REGISTROS = df_eval.count()

print(f"   Avaliando run_id : {run_mais_recente}")
print(f"   Total de registros : {TOTAL_REGISTROS:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 5] DIMENSÃO 1 — Completude

# COMMAND ----------

registros_quality = []

# Colunas obrigatórias e essenciais para avaliação de completude
COLUNAS_COMPLETUDE = {
    "nu_notific":  "Número de notificação — chave natural do caso. Nulos impedem deduplicação e rastreabilidade.",
    "dt_notific":  "Data de notificação obrigatória. Nulos afetam séries temporais e watermark.",
    "dt_sin_pri":  "Data dos primeiros sintomas — eixo temporal principal do dashboard.",
    "sg_uf":       "UF de residência — essencial para análise geográfica.",
    "co_mun_res":  "Município de residência — granularidade geográfica.",
    "classi_fin":  "Classificação final SRAG — determina agente etiológico. Nulos afetam todas as taxas.",
    "evolucao":    "Evolução do caso — indispensável para taxa de mortalidade.",
    "uti":         "Indicador de UTI — indispensável para taxa de uso de UTI.",
    "vacina":      "Vacinação contra gripe — denominator da taxa de vacinação gripe.",
    "vacina_cov":  "Vacinação COVID-19 — denominator da taxa de vacinação COVID.",
    "hospital":    "Hospitalização — denominator da taxa de uso de UTI.",
}

print(" Dimensão 1 — Completude:")
for coluna, obs in COLUNAS_COMPLETUDE.items():
    col_real = coluna
    validos = df_eval.filter(F.col(col_real).isNotNull() & (F.trim(F.col(col_real)) != "")).count()
    reg = criar_registro_qualidade(
        regra=f"Completude - {col_real}",
        dimensao="Completude",
        coluna=col_real,
        total=TOTAL_REGISTROS,
        validos=validos,
        observacao=obs,
    )
    registros_quality.append(reg)
    print(f"   {reg['severidade']:10s} | {col_real:25s} | {reg['percentual_validade']*100:.1f}% válidos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 6] DIMENSÃO 2 — Validade de Domínio

# COMMAND ----------

print(" Dimensão 2 — Validade de Domínio:")

# Mapeamento: coluna → (valores válidos, descrição)
DOMINIOS = {
    "cs_sexo":    (["M", "F", "I", "1", "2", "9"],
                   "Sexo: M=Masc, F=Fem, I=Ignorado (ou 1,2,9 conforme DBF)."),
    "tp_idade":   (["1", "2", "3"],
                   "Tipo idade: 1=Dia, 2=Mês, 3=Ano."),
    "cs_gestant": (["1","2","3","4","5","6","9"],
                   "Gestante: 1-6=trimestres/não/n.a., 9=Ignorado."),
    "cs_raca":    (["1","2","3","4","5","9"],
                   "Raça/cor: 1=Branca, 2=Preta, 3=Amarela, 4=Parda, 5=Indígena, 9=Ignorado."),
    "febre":      (["1","2","9"],  "Febre: 1=Sim, 2=Não, 9=Ignorado."),
    "tosse":      (["1","2","9"],  "Tosse: 1=Sim, 2=Não, 9=Ignorado."),
    "dispneia":   (["1","2","9"],  "Dispneia: 1=Sim, 2=Não, 9=Ignorado."),
    "saturacao":  (["1","2","9"],  "Saturação O2<95%: 1=Sim, 2=Não, 9=Ignorado."),
    "fator_risc": (["1","2","9"],  "Fator de risco: 1=Sim, 2=Não, 9=Ignorado."),
    "vacina":     (["1","2","9"],  "Vacina gripe: 1=Sim, 2=Não, 9=Ignorado."),
    "vacina_cov": (["1","2","9"],  "Vacina COVID: 1=Sim, 2=Não, 9=Ignorado."),
    "uti":        (["1","2","9"],  "UTI: 1=Sim, 2=Não, 9=Ignorado."),
    "hospital":   (["1","2","9"],  "Hospitalização: 1=Sim, 2=Não, 9=Ignorado."),
    "evolucao":   (["1","2","3","9"],
                   "Evolução: 1=Cura, 2=Óbito, 3=Óbito outras causas, 9=Ignorado."),
    "classi_fin": (["1","2","3","4","5"],
                   "Classificação final: 1=Influenza, 2=Outro vírus, 3=Outro agente, 4=Não espec., 5=COVID-19."),
    "suport_ven": (["1","2","3","9"],
                   "Suporte ventilatório: 1=Invasivo, 2=Não invasivo, 3=Não, 9=Ignorado."),
    "pcr_resul":  (["1","2","3","4","5","9"],
                   "Resultado PCR: 1=Detectável, 2=Não detectável, 3=Inconclusivo, 4=Não realizado, 5=Aguardando."),
    "criterio":   (["1","2","3","4"],
                   "Critério encerramento: 1=Laboratorial, 2=Clínico Epidemiológico, 3=Clínico, 4=Clínico Imagem."),
}

for coluna, (valores_validos, obs) in DOMINIOS.items():
    col_trimmed = F.trim(F.upper(F.col(coluna)))
    validos = (
        df_eval
        .filter(F.col(coluna).isNull() |
                col_trimmed.isin(valores_validos))
        .count()
    )
    reg = criar_registro_qualidade(
        regra=f"Validade Domínio - {coluna}",
        dimensao="Validade",
        coluna=coluna,
        total=TOTAL_REGISTROS,
        validos=validos,
        observacao=obs,
    )
    registros_quality.append(reg)
    print(f"   {reg['severidade']:10s} | {coluna:25s} | {reg['percentual_validade']*100:.1f}% válidos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 7] DIMENSÃO 3 — Consistência Temporal

# COMMAND ----------

print(" Dimensão 3 — Consistência Temporal:")

# Converter datas da Bronze (STRING) para date em memória para comparação
df_datas = df_eval.select(
    "nu_notific",
    F.expr("try_to_date(substring(dt_notific, 1, 10), 'yyyy-MM-dd')").alias("dt_notific_d"),
    F.expr("try_to_date(substring(dt_sin_pri, 1, 10), 'yyyy-MM-dd')").alias("dt_sin_pri_d"),
    F.expr("try_to_date(substring(dt_interna, 1, 10), 'yyyy-MM-dd')").alias("dt_interna_d"),
    F.expr("try_to_date(substring(dt_entuti, 1, 10), 'yyyy-MM-dd')").alias("dt_entuti_d"),
    F.expr("try_to_date(substring(dt_saiduti, 1, 10), 'yyyy-MM-dd')").alias("dt_saiduti_d"),
    F.expr("try_to_date(substring(dt_evoluca, 1, 10), 'yyyy-MM-dd')").alias("dt_evoluca_d"),
    F.expr("try_to_date(substring(dt_encerra, 1, 10), 'yyyy-MM-dd')").alias("dt_encerra_d"),
)

REGRAS_TEMPORAIS = [
    # (nome_regra, condição_válida, colunas_envolvidas, observação)
    (
        "dt_sin_pri <= dt_notific",
        F.col("dt_sin_pri_d").isNull() | F.col("dt_notific_d").isNull() |
        (F.col("dt_sin_pri_d") <= F.col("dt_notific_d")),
        "dt_sin_pri / dt_notific",
        "Primeiros sintomas não podem ser posteriores à notificação.",
    ),
    (
        "dt_interna >= dt_sin_pri",
        F.col("dt_interna_d").isNull() | F.col("dt_sin_pri_d").isNull() |
        (F.col("dt_interna_d") >= F.col("dt_sin_pri_d")),
        "dt_interna / dt_sin_pri",
        "Internação não pode ser anterior aos primeiros sintomas.",
    ),
    (
        "dt_entuti >= dt_sin_pri",
        F.col("dt_entuti_d").isNull() | F.col("dt_sin_pri_d").isNull() |
        (F.col("dt_entuti_d") >= F.col("dt_sin_pri_d")),
        "dt_entuti / dt_sin_pri",
        "Entrada UTI não pode ser anterior aos primeiros sintomas.",
    ),
    (
        "dt_saiduti >= dt_entuti",
        F.col("dt_saiduti_d").isNull() | F.col("dt_entuti_d").isNull() |
        (F.col("dt_saiduti_d") >= F.col("dt_entuti_d")),
        "dt_saiduti / dt_entuti",
        "Saída UTI não pode ser anterior à entrada UTI.",
    ),
    (
        "dt_evoluca >= dt_sin_pri",
        F.col("dt_evoluca_d").isNull() | F.col("dt_sin_pri_d").isNull() |
        (F.col("dt_evoluca_d") >= F.col("dt_sin_pri_d")),
        "dt_evoluca / dt_sin_pri",
        "Alta/óbito não pode ser anterior aos primeiros sintomas.",
    ),
    (
        "dt_encerra >= dt_notific",
        F.col("dt_encerra_d").isNull() | F.col("dt_notific_d").isNull() |
        (F.col("dt_encerra_d") >= F.col("dt_notific_d")),
        "dt_encerra / dt_notific",
        "Encerramento não pode ser anterior à data de notificação.",
    ),
]

for nome_regra, condicao, colunas, obs in REGRAS_TEMPORAIS:
    validos = df_datas.filter(condicao).count()
    reg = criar_registro_qualidade(
        regra=f"Consistência Temporal - {nome_regra}",
        dimensao="Consistência",
        coluna=colunas,
        total=TOTAL_REGISTROS,
        validos=validos,
        observacao=obs,
    )
    registros_quality.append(reg)
    print(f"   {reg['severidade']:10s} | {nome_regra:40s} | {reg['percentual_validade']*100:.1f}% válidos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 8] DIMENSÃO 4 — Unicidade

# COMMAND ----------

print(" Dimensão 4 — Unicidade:")

# 4a. Duplicidade por nu_notific
total_com_notific = df_eval.filter(F.col("nu_notific").isNotNull()).count()
distintos_notific  = df_eval.filter(F.col("nu_notific").isNotNull()) \
                            .select("nu_notific").distinct().count()
duplicatas_notific = total_com_notific - distintos_notific

reg_uni_1 = criar_registro_qualidade(
    regra="Unicidade - nu_notific",
    dimensao="Unicidade",
    coluna="nu_notific",
    total=total_com_notific,
    validos=distintos_notific,
    observacao=(
        f"Duplicatas detectadas: {duplicatas_notific:,}. "
        "Duplicatas em nu_notific indicam reenvio de mesmo caso. "
        "A Silver aplicará SCD2 mantendo apenas o registro mais recente."
    ),
)
registros_quality.append(reg_uni_1)
print(f"   {reg_uni_1['severidade']:10s} | nu_notific (único)          | {distintos_notific:,} distintos / {total_com_notific:,} com valor")
print(f"             | Duplicatas detectadas      : {duplicatas_notific:,}")

# 4b. Duplicidade por combinação nu_notific + dt_sin_pri + dt_notific
df_chave_composta = df_eval.filter(
    F.col("nu_notific").isNotNull() &
    F.col("dt_sin_pri").isNotNull() &
    F.col("dt_notific").isNotNull()
)
total_composta    = df_chave_composta.count()
distintos_composta = df_chave_composta \
    .select("nu_notific", "dt_sin_pri", "dt_notific").distinct().count()
duplicatas_composta = total_composta - distintos_composta

reg_uni_2 = criar_registro_qualidade(
    regra="Unicidade - nu_notific + dt_sin_pri + dt_notific",
    dimensao="Unicidade",
    coluna="nu_notific / dt_sin_pri / dt_notific",
    total=total_composta,
    validos=distintos_composta,
    observacao=(
        f"Duplicatas na chave composta: {duplicatas_composta:,}. "
        "Indica múltiplas linhas para o mesmo caso com mesmas datas — possível reenvio."
    ),
)
registros_quality.append(reg_uni_2)
print(f"   {reg_uni_2['severidade']:10s} | Chave composta (3 campos)   | {distintos_composta:,} distintos / {total_composta:,} com valor")
print(f"             | Duplicatas detectadas      : {duplicatas_composta:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 9] DIMENSÃO 5 — Atualidade

# COMMAND ----------

print(" Dimensão 5 — Atualidade:")

from pyspark.sql.functions import max as spark_max, datediff, current_date

df_datas_atu = df_eval.select(
    F.expr("try_to_date(substring(dt_sin_pri, 1, 10), 'yyyy-MM-dd')").alias("dt_sin_pri_d"),
    F.expr("try_to_date(substring(dt_notific, 1, 10), 'yyyy-MM-dd')").alias("dt_notific_d"),
    F.expr("try_to_date(substring(dt_digita, 1, 10), 'yyyy-MM-dd')").alias("dt_digita_d"),
)

row_max = df_datas_atu.agg(
    spark_max("dt_sin_pri_d").alias("max_sin_pri"),
    spark_max("dt_notific_d").alias("max_notific"),
    spark_max("dt_digita_d").alias("max_digita"),
).first()

max_sin_pri = row_max["max_sin_pri"]
max_notific = row_max["max_notific"]
max_digita  = row_max["max_digita"]
hoje        = date.today()

def dias_defasagem(data_max):
    if data_max is None:
        return None
    return (hoje - data_max).days

defasagem_sin  = dias_defasagem(max_sin_pri)
defasagem_not  = dias_defasagem(max_notific)
defasagem_dig  = dias_defasagem(max_digita)

print(f"   Data máxima dt_sin_pri  : {max_sin_pri} (defasagem: {defasagem_sin} dias)")
print(f"   Data máxima dt_notific  : {max_notific} (defasagem: {defasagem_not} dias)")
print(f"   Data máxima dt_digita   : {max_digita}  (defasagem: {defasagem_dig} dias)")
print(f"   Data de execução        : {hoje}")

# Criar registros de atualidade (válido = defasagem <= 30 dias)
def validar_atualidade(defasagem, coluna, obs_extra=""):
    valido = 1 if (defasagem is not None and defasagem <= 30) else 0
    return criar_registro_qualidade(
        regra=f"Atualidade - {coluna}",
        dimensao="Atualidade",
        coluna=coluna,
        total=1,
        validos=valido,
        observacao=(
            f"Data máxima: {max_sin_pri if 'sin' in coluna else (max_notific if 'notific' in coluna else max_digita)}. "
            f"Defasagem: {defasagem} dias. "
            f"Limite aceitável: 30 dias. {obs_extra}"
        ),
    )

registros_quality.append(validar_atualidade(
    defasagem_sin, "dt_sin_pri",
    "Defasagem elevada pode indicar atraso de digitação no SIVEP-Gripe."
))
registros_quality.append(validar_atualidade(
    defasagem_not, "dt_notific",
    "Notificações recentes podem ainda estar em digitação."
))
registros_quality.append(validar_atualidade(
    defasagem_dig, "dt_digita",
    "Data de digitação reflete quando o registro entrou no sistema."
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 10] Consolidação e Gravação da Tabela de Qualidade

# COMMAND ----------

print(f"\n Total de regras avaliadas: {len(registros_quality)}")
print(f"   Distribuição por severidade:")

# Criar DataFrame de qualidade
df_quality = spark.createDataFrame(registros_quality, schema=SCHEMA_QUALITY)

# Exibir resumo por severidade
resumo = (
    df_quality.groupBy("severidade")
    .agg(F.count("*").alias("total_regras"))
    .orderBy(
        F.when(F.col("severidade")=="CRÍTICO", 0)
         .when(F.col("severidade")=="ALTO", 1)
         .when(F.col("severidade")=="MÉDIO", 2)
         .when(F.col("severidade")=="BAIXO", 3)
         .otherwise(4)
    )
)
resumo.show(truncate=False)

# Gravar em APPEND — série histórica de execuções
print(f" Gravando em: {FULL_QUALITY}")
(
    df_quality
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(FULL_QUALITY)
)

print(f" Qualidade gravada com sucesso.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 11] Relatório Consolidado de Qualidade

# COMMAND ----------

print("\n" + "="*80)
print("  RELATÓRIO DE QUALIDADE BRONZE — SIVEP-Gripe SRAG 2025")
print("="*80)

# Alertas críticos e altos
alertas = df_quality.filter(F.col("severidade").isin(["CRÍTICO", "ALTO"]))
total_alertas = alertas.count()

if total_alertas > 0:
    print(f"\n⚠️  {total_alertas} alertas detectados (CRÍTICO ou ALTO):")
    alertas.select(
        "severidade", "dimensao_dmbok", "coluna_avaliada",
        (F.round(F.col("percentual_validade") * 100, 1)).alias("pct_valido_%"),
        "registros_invalidos"
    ).orderBy("severidade", "percentual_validade").show(50, truncate=False)
else:
    print("\n Nenhum alerta CRÍTICO ou ALTO detectado.")

# Resumo geral
print(f"\n Resumo Geral:")
print(f"   Total de registros avaliados : {TOTAL_REGISTROS:,}")
print(f"   Total de regras aplicadas    : {len(registros_quality)}")
print(f"\n   Data/hora de execução        : {datetime.utcnow().isoformat()}Z")
print(f"   Lote avaliado (run_id)       : {run_mais_recente}")

# Impacto nas métricas Gold
print(f"""
 Impacto Analítico Esperado:
   • Completude de 'classi_fin'  → afeta TODAS as taxas de classificação SRAG
   • Completude de 'evolucao'    → afeta taxa de mortalidade
   • Completude de 'uti'         → afeta taxa de uso de UTI
   • Completude de 'vacina'      → afeta taxa de vacinação gripe
   • Completude de 'vacina_cov'  → afeta taxa de vacinação COVID
   • Duplicatas em 'nu_notific'  → afeta contagem total de casos (Silver deduplica)
   • Consistência temporal       → datas inconsistentes excluídas das séries temporais
""")

print("="*80)
print(f" BRONZE QUALITY CONCLUÍDA | regras={len(registros_quality)} | alertas_criticos_altos={total_alertas}")
print("="*80)

dbutils.notebook.exit(
    f"SUCESSO|regras={len(registros_quality)}|alertas_criticos_altos={total_alertas}"
)