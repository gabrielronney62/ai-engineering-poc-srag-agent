# Databricks notebook source
# MAGIC %md
# MAGIC # 03b — Silver: Leitos UTI CNES 2025/2026
# MAGIC
# MAGIC **PoC:** HealthCare Gabriel Ronney — Monitoramento de SRAG
# MAGIC **Camada:** Silver
# MAGIC **Origem:** `bronze_dados_leitos_2024_2026`
# MAGIC **Destino:** `silver_cnes_leitos_uti`
# MAGIC **Estratégia:** OVERWRITE (dado reflete snapshot mensal por estabelecimento)
# MAGIC
# MAGIC ---
# MAGIC ### O que este notebook faz
# MAGIC
# MAGIC 1. Lê a `bronze_dados_leitos_2024_2026` (criada pelo usuário no Databricks)
# MAGIC 2. Sanitiza nomes de colunas → snake_case
# MAGIC 3. Remove colunas PII (endereço, telefone, e-mail)
# MAGIC 4. Aplica tipagem correta (integer para leitos, string para identificadores)
# MAGIC 5. Trata diferença de schema entre 2025 (sem CO_IBGE) e 2026 (com CO_IBGE)
# MAGIC 6. Agrega capacidade de UTI por UF + competência (ano_mes)
# MAGIC    → Esta agregação será o denominador real da taxa de ocupação de UTI
# MAGIC 7. Adiciona colunas técnicas obrigatórias do Playbook
# MAGIC 8. Grava em OVERWRITE (snapshot mensal — reprocessamento seguro)
# MAGIC
# MAGIC ---
# MAGIC ### Por que OVERWRITE (não MERGE/SCD2)?
# MAGIC
# MAGIC Os dados de leitos são publicados mensalmente pelo CNES com o total
# MAGIC existente naquele mês. Não há histórico de vigência por registro —
# MAGIC o dado de Jan/2025 é sempre o mesmo, só o mês seguinte traz um novo snapshot.
# MAGIC Conforme Playbook §1.2 (exceção controlada para Silver):
# MAGIC - A tabela é derivada integralmente da Bronze (reprocessamento sempre possível)
# MAGIC - Não há dependência de SCD Tipo 2
# MAGIC - O volume permite reconstrução completa dentro do SLA
# MAGIC
# MAGIC ---
# MAGIC ### JOIN com SRAG para a nova métrica
# MAGIC
# MAGIC | Silver Leitos | Silver SRAG | Resultado |
# MAGIC |---|---|---|
# MAGIC | `uf` + `ano_mes` | `sg_uf_residencia` + `year(dt_sin_pri)` + `month(dt_sin_pri)` | `taxa_ocupacao_uti_real` |
# MAGIC | `uti_total_exist` (capacidade) | `flag_uti` (uso por SRAG) | `uti_srag / uti_total_exist` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 1] Setup

# COMMAND ----------

import re
import uuid
import unicodedata
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType

RUN_ID             = str(uuid.uuid4())
DH_INICIO_EXECUCAO = datetime.now(timezone.utc)

CATALOGO     = "certificacao_indicium"
SCHEMA       = "poc_srag_datasus"
FULL_BRONZE  = f"{CATALOGO}.{SCHEMA}.bronze_dados_leitos_2024_2026"
FULL_SILVER  = f"{CATALOGO}.{SCHEMA}.silver_cnes_leitos_uti"

print(f" run_id  : {RUN_ID}")
print(f" Bronze  : {FULL_BRONZE}")
print(f" Silver  : {FULL_SILVER}")
print(f" Início  : {DH_INICIO_EXECUCAO.isoformat()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 2] Funções Utilitárias — Sanitização

# COMMAND ----------

def sanitizar_snake_case(nome: str) -> str:
    """
    Converte nome de coluna para snake_case conforme Playbook §4.1:
    - Remove acentos
    - Converte para minúsculas
    - Substitui espaços, hífens e caracteres especiais por underscore
    - Remove underscores duplicados e nas bordas

    Exemplos:
        'UTI_TOTAL_EXIST'   → 'uti_total_exist'
        'NOME ESTABELECIMENTO' → 'nome_estabelecimento'
        'RAZÃO SOCIAL'      → 'razao_social'
        'CO_IBGE'           → 'co_ibge'
    """
    nfkd       = unicodedata.normalize("NFKD", nome)
    sem_acento = "".join(c for c in nfkd if not unicodedata.combining(c))
    lower      = sem_acento.lower()
    limpo      = re.sub(r"[\s\-\.\/\\()]+", "_", lower)
    apenas_aln = re.sub(r"[^a-z0-9_]", "", limpo)
    return re.sub(r"_+", "_", apenas_aln).strip("_")


def sanitizar_colunas_df(df):
    """
    Aplica sanitizar_snake_case em todos os nomes de colunas.
    Garante unicidade em caso de colisão após conversão.
    """
    vistas = []
    for col_orig in df.columns:
        col_nova = sanitizar_snake_case(col_orig)
        if col_nova in vistas:
            col_nova = col_nova + "_dup"
        vistas.append(col_nova)
        if col_orig != col_nova:
            df = df.withColumnRenamed(col_orig, col_nova)
    return df


print(" Funções de sanitização definidas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 3] Leitura da Bronze e Diagnóstico de Schema

# COMMAND ----------

df_bronze = spark.table(FULL_BRONZE)

print(f" Bronze lida: {df_bronze.count():,} registros")
print(f"   Colunas    : {len(df_bronze.columns)}")
print()

# Verificar colunas como chegam da Bronze (ainda em MAIÚSCULAS se não houve sanitização)
print("Colunas da Bronze (primeiras 20):")
for c in df_bronze.columns[:20]:
    print(f"  {c}")

print()
# Verificar período coberto
comp_vals = df_bronze.select("COMP").distinct().orderBy("COMP").collect() \
            if "COMP" in df_bronze.columns \
            else df_bronze.select("comp").distinct().orderBy("comp").collect()
print(f"Competências (COMP) presentes: {[r[0] for r in comp_vals[:5]]} ... {[r[0] for r in comp_vals[-3:]]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 4] Sanitização de Colunas → snake_case
# MAGIC
# MAGIC A `bronze_dados_leitos_2024_2026` foi criada com colunas em MAIÚSCULAS
# MAGIC conforme os CSVs originais do CNES. A sanitização acontece aqui na Silver,
# MAGIC mantendo a Bronze intacta (não quebramos o que já existe).

# COMMAND ----------

df_sanitizado = sanitizar_colunas_df(df_bronze)

print(" Sanitização de colunas concluída:")
print()

# Mostrar mapeamento completo
bronze_cols = df_bronze.columns
silver_cols = df_sanitizado.columns
for orig, nova in zip(bronze_cols, silver_cols):
    status = "→ " if orig != nova else "  "
    print(f"  {status} {orig:35s} → {nova}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 5] Colunas PII — Excluir da Silver

# COMMAND ----------

# Colunas PII/irrelevantes que NÃO devem ir para a Silver
# (endereço completo, contato, informações que identificam o estabelecimento individualmente)
COLUNAS_PII_LEITOS = [
    "no_logradouro",   # endereço
    "nu_endereco",     # número
    "no_complemento",  # complemento
    "no_bairro",       # bairro
    "co_cep",          # CEP
    "nu_telefone",     # telefone
    "no_email",        # e-mail
    # Razão social e nome fantasia: mantemos nome_estabelecimento para contexto analítico
    # mas removemos razao_social (redundante e pode identificar entidades específicas)
    "razao_social",
    # Motivo de desabilitação: texto livre, sem uso analítico nesta PoC
    "motivo_desabilitacao",
]

cols_pii_presentes = [c for c in COLUNAS_PII_LEITOS if c in df_sanitizado.columns]
cols_manter = [c for c in df_sanitizado.columns if c not in COLUNAS_PII_LEITOS]

df_sem_pii = df_sanitizado.select(cols_manter)

print(f" Colunas removidas (PII/irrelevante): {len(cols_pii_presentes)}")
for c in cols_pii_presentes:
    print(f"   ✗ {c}")
print()
print(f" Colunas mantidas: {len(cols_manter)}")
for c in cols_manter:
    print(f"   ✓ {c}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 6] Tipagem e Transformações

# COMMAND ----------

df_typed = df_sem_pii

# ── Tratar CO_IBGE: presente apenas no 2026, nulo no 2025 ─────────────────────
# A Bronze unificada já deve ter a coluna; se não tiver, criar com null
if "co_ibge" not in df_typed.columns:
    df_typed = df_typed.withColumn("co_ibge", F.lit(None).cast(StringType()))
    print("⚠️  Coluna co_ibge ausente na Bronze — adicionada como NULL")
else:
    df_typed = df_typed.withColumn("co_ibge", F.col("co_ibge").cast(StringType()))

# ── Padronização de texto ──────────────────────────────────────────────────────
# UF e REGIAO: uppercase
for c in ["uf", "regiao"]:
    if c in df_typed.columns:
        df_typed = df_typed.withColumn(c, F.upper(F.trim(F.col(c))))

# MUNICIPIO: Title Case
if "municipio" in df_typed.columns:
    df_typed = df_typed.withColumn(
        "municipio",
        F.initcap(F.trim(F.col("municipio")))
    )

# NOME_ESTABELECIMENTO: Title Case
if "nome_estabelecimento" in df_typed.columns:
    df_typed = df_typed.withColumn(
        "nome_estabelecimento",
        F.initcap(F.trim(F.col("nome_estabelecimento")))
    )

# ── Tipagem de campos numéricos (leitos) ───────────────────────────────────────
COLUNAS_LEITOS_INT = [
    "leitos_existente", "leitos_sus",
    "uti_total_exist",   "uti_total_sus",
    "uti_adulto_exist",  "uti_adulto_sus",
    "uti_pediatrico_exist", "uti_pediatrico_sus",
    "uti_neonatal_exist",   "uti_neonatal_sus",
    "uti_queimado_exist",   "uti_queimado_sus",
    "uti_coronariana_exist","uti_coronariana_sus",
]
for c in COLUNAS_LEITOS_INT:
    if c in df_typed.columns:
        df_typed = df_typed.withColumn(c, F.expr(f"try_cast(`{c}` as int)"))

# ── Derivar campo ano_mes a partir de COMP ─────────────────────────────────────
# COMP vem como integer (ex: 202501) → derivar ano_mes no formato 'yyyy-MM'
# e também ano e mes separados para facilitar joins com SRAG
df_typed = df_typed.withColumn("comp_str",  F.col("comp").cast(StringType()))
df_typed = df_typed.withColumn("ano",        F.col("comp_str").substr(1, 4).cast(IntegerType()))
df_typed = df_typed.withColumn("mes",        F.col("comp_str").substr(5, 2).cast(IntegerType()))
df_typed = df_typed.withColumn(
    "ano_mes",
    F.concat(
        F.col("comp_str").substr(1, 4),
        F.lit("-"),
        F.col("comp_str").substr(5, 2)
    )
)
df_typed = df_typed.drop("comp_str")

# ── Derivar classificação de gestão ───────────────────────────────────────────
if "tp_gestao" in df_typed.columns:
    df_typed = df_typed.withColumn(
        "desc_tp_gestao",
        F.when(F.col("tp_gestao") == "M", F.lit("Municipal"))
         .when(F.col("tp_gestao") == "E", F.lit("Estadual"))
         .when(F.col("tp_gestao") == "D", F.lit("Dupla"))
         .when(F.col("tp_gestao") == "S", F.lit("Sem Gestão"))
         .otherwise(F.lit("Não informado"))
    )

print(f"✅ Tipagem e transformações concluídas.")
print(f"   Total registros: {df_typed.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 7] Agregar por UF + Ano_Mes
# MAGIC
# MAGIC Esta é a tabela que o agente vai usar para calcular a taxa real de ocupação
# MAGIC de UTI. O grão final é **UF + ano_mes** — capacidade total de leitos UTI
# MAGIC por estado por mês.
# MAGIC
# MAGIC **Por que agregar?**
# MAGIC - Os dados SRAG estão no nível de UF (não CNES individual)
# MAGIC - Para o JOIN com SRAG precisamos da capacidade total por UF por mês
# MAGIC - Um estabelecimento individual não representa a capacidade do estado inteiro

# COMMAND ----------

# Manter tabela granular (por estabelecimento) E tabela agregada (por UF/mês)

# ── Tabela granular — 1 linha por estabelecimento por competência ──────────────
df_granular = df_typed.withColumn("dh_ingestao_silver", F.current_timestamp()) \
                       .withColumn("run_id",              F.lit(RUN_ID))

# ── Agregada por UF + ano_mes — denominador para a métrica de ocupação ─────────
df_agg_uf = (
    df_typed
    .groupBy("uf", "regiao", "ano", "mes", "ano_mes")
    .agg(
        # Leitos totais
        F.sum("leitos_existentes").alias("leitos_existentes_total"),
        F.sum("leitos_sus").alias("leitos_sus_total"),
        # UTI — Total (somatório adulto + pediátrico + neonatal + queimado + coronariana)
        F.sum("uti_total_exist").alias("uti_total_exist"),
        F.sum("uti_total_sus").alias("uti_total_sus"),
        # UTI — Tipos (para análise granular)
        F.sum("uti_adulto_exist").alias("uti_adulto_exist"),
        F.sum("uti_adulto_sus").alias("uti_adulto_sus"),
        F.sum("uti_pediatrico_exist").alias("uti_pediatrico_exist"),
        F.sum("uti_pediatrico_sus").alias("uti_pediatrico_sus"),
        F.sum("uti_neonatal_exist").alias("uti_neonatal_exist"),
        F.sum("uti_neonatal_sus").alias("uti_neonatal_sus"),
        F.sum("uti_queimado_exist").alias("uti_queimado_exist"),
        F.sum("uti_queimado_sus").alias("uti_queimado_sus"),
        F.sum("uti_coronariana_exist").alias("uti_coronariana_exist"),
        F.sum("uti_coronariana_sus").alias("uti_coronariana_sus"),
        # Metadados
        F.countDistinct("cnes").alias("qtd_estabelecimentos"),
    )
    .withColumn("dh_ingestao_silver", F.current_timestamp())
    .withColumn("run_id",              F.lit(RUN_ID))
)

total_granular = df_granular.count()
total_agg      = df_agg_uf.count()

print(f"✅ Tabelas construídas:")
print(f"   Granular (por estabelecimento): {total_granular:,} linhas")
print(f"   Agregada (por UF + ano_mes)   : {total_agg:,} linhas")
print()

# Preview da agregação
print("Preview da agregação UF + ano_mes (Jan/2025):")
df_agg_uf.filter(F.col("ano_mes") == "2025-01") \
    .select("uf", "ano_mes", "uti_total_exist", "uti_total_sus",
            "leitos_existentes_total", "qtd_estabelecimentos") \
    .orderBy(F.col("uti_total_exist").desc()) \
    .show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 8] Gravação — OVERWRITE

# COMMAND ----------

# Gravar Silver granular (por estabelecimento)
(
    df_granular
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_SILVER)
)
print(f"✅ {FULL_SILVER} gravada: {total_granular:,} registros")

# Gravar Silver agregada por UF + ano_mes (denominador para taxa real de UTI)
FULL_SILVER_AGG = f"{CATALOGO}.{SCHEMA}.silver_cnes_leitos_uti_por_uf"
(
    df_agg_uf
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_SILVER_AGG)
)
print(f"✅ {FULL_SILVER_AGG} gravada: {total_agg:,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 9] Validações

# COMMAND ----------

# Reconstruct FULL_SILVER_AGG reference (lost after kernel restart in Cell 17)
FULL_SILVER_AGG = f"{CATALOGO}.{SCHEMA}.silver_cnes_leitos_uti_por_uf"

# Validar cobertura de período na Silver
print(" Cobertura de período na Silver:")
spark.table(FULL_SILVER_AGG) \
    .groupBy("ano") \
    .agg(
        F.countDistinct("mes").alias("meses_cobertos"),
        F.min("ano_mes").alias("primeiro_mes"),
        F.max("ano_mes").alias("ultimo_mes"),
        F.sum("uti_total_exist").alias("total_uti_exist")
    ) \
    .orderBy("ano").show()

# Validar ausência de nulos em campos críticos
df_silver = spark.table(FULL_SILVER_AGG)
total = df_silver.count()
nulos_uf = df_silver.filter(F.col("uf").isNull()).count()
nulos_uti = df_silver.filter(F.col("uti_total_exist").isNull()).count()
print(f"Nulos em 'uf'           : {nulos_uf} / {total}")
print(f"Nulos em 'uti_total_exist': {nulos_uti} / {total}")

assert nulos_uf == 0,  "ERRO: Registros sem UF na Silver!"
print("\n Todas as validações passaram.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 10] Otimização

# COMMAND ----------

spark.sql(f"OPTIMIZE {FULL_SILVER} ZORDER BY (uf, ano_mes)")
spark.sql(f"OPTIMIZE {FULL_SILVER_AGG} ZORDER BY (uf, ano_mes)")
print(" OPTIMIZE + Z-ORDER concluídos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 11] Saída

# COMMAND ----------

print(f"\n{'='*60}")
print(f"   SILVER LEITOS UTI CONCLUÍDA")
print(f"  Granular: {FULL_SILVER}")
print(f"            {total_granular:,} registros (por estabelecimento)")
print(f"  Agregada: {FULL_SILVER_AGG}")
print(f"            {total_agg:,} registros (por UF + ano_mes)")
print(f"  run_id  : {RUN_ID}")
print(f"  Início  : {DH_INICIO_EXECUCAO.isoformat()}")
print(f"{'='*60}")
print()
print("PRÓXIMO PASSO:")
print("  Rodar 04_gold_modelagem_srag.py — que irá usar")
print(f"  {FULL_SILVER_AGG}")
print("  para calcular a taxa real de ocupação de UTI.")

dbutils.notebook.exit(
    f"SUCESSO|run_id={RUN_ID}|granular={total_granular}|agg_uf={total_agg}"
)
