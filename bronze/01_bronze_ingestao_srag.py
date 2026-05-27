# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze: Ingestão SRAG DATASUS (Auto Loader — Multi-arquivo)
# MAGIC
# MAGIC **PoC:** HealthCare — Monitoramento de SRAG
# MAGIC **Camada:** Bronze
# MAGIC **Estratégia:** APPEND imutável
# MAGIC **Ingestão:** Auto Loader (`cloudFiles`) — monitora o Volume raw automaticamente
# MAGIC
# MAGIC ---
# MAGIC ### Arquivos no Volume raw
# MAGIC
# MAGIC | Arquivo | Registros | Período | Formato de data |
# MAGIC |---|---|---|---|
# MAGIC | `INFLUD25_DATASUS-Versao26-06-2025.csv` | 165.397 | Dez/2024 → Jun/2025 | `YYYY-MM-DD` |
# MAGIC | `INFLUD25-2025.csv` | 336.239 | Dez/2024 → Jan/2026 | `YYYY-MM-DDTHH:MM:SS.mmmZ` |
# MAGIC | `INFLUD26-2026.csv` | 101.425 | Jan/2026 → Mai/2026 | `YYYY-MM-DDTHH:MM:SS.mmmZ` |
# MAGIC
# MAGIC **Total esperado sem duplicatas (após SCD2 na Silver): ~440.674 registros**
# MAGIC
# MAGIC ---
# MAGIC **Solução:** Na Bronze, todas as colunas permanecem STRING,
# MAGIC porém adicionamos a coluna `formato_data_origem` para documentar a diferença.
# MAGIC A normalização para o formato único `YYYY-MM-DD` ocorre na **Silver**, conforme
# MAGIC o fluxo correto da Medallion Architecture.
# MAGIC
# MAGIC **A Bronze preserva o dado exatamente como veio da fonte — sem alterar nenhum valor.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 1] Setup e Dependências

# COMMAND ----------

import re
import uuid
import unicodedata
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

print(" Bibliotecas carregadas.")
print(f"   Spark version : {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 2] Parâmetros e Variáveis de Controle

# COMMAND ----------

# ── Identificação da Execução ──────────────────────────────────────────────────
RUN_ID             = str(uuid.uuid4())
DH_INICIO_EXECUCAO = datetime.now(timezone.utc)

# ── Origem — Volume raw monitorado pelo Auto Loader ────────────────────────────
VOLUME_RAW_PATH     = (
    "/Volumes/certificacao_indicium/poc_srag_datasus"
    "/volume_poc_srag_datasus/raw"
)
SEPARADOR_CSV       = ";"
NOME_SISTEMA_ORIGEM = "Open DATASUS - SIVEP-Gripe"

# ── Mapeamento de versões por arquivo ──────────────────────────────────────────
# Usado para popular versao_arquivo_origem por nome de arquivo
VERSOES_ARQUIVO = {
    "INFLUD25_DATASUS-Versao26-06-2025.csv": "26-06-2025",
    "INFLUD25-2025.csv":                     "2025",
    "INFLUD26-2026.csv":                     "2026",
}

# ── Checkpoint e Schema Hints ──────────────────────────────────────────────────
CHECKPOINT_PATH   = (
    "/Volumes/certificacao_indicium/poc_srag_datasus"
    "/volume_poc_srag_datasus/_checkpoints/bronze_autoloader"
)
SCHEMA_HINTS_PATH = (
    "/Volumes/certificacao_indicium/poc_srag_datasus"
    "/volume_poc_srag_datasus/_schema_hints/bronze_autoloader"
)

# ── Destino ────────────────────────────────────────────────────────────────────
CATALOGO        = "certificacao_indicium"
SCHEMA          = "poc_srag_datasus"
TABELA_BRONZE   = "bronze_poc_srag_datasus"
FULL_TABLE_NAME = f"{CATALOGO}.{SCHEMA}.{TABELA_BRONZE}"

# ── Trigger ───────────────────────────────────────────────────────────────────
# "once" → processa todos os arquivos novos e para (ideal para Workflow agendado)
TRIGGER_ONCE = True

print(f" run_id              : {RUN_ID}")
print(f" Volume monitorado   : {VOLUME_RAW_PATH}")
print(f" Tabela destino      : {FULL_TABLE_NAME}")
print(f" Início              : {DH_INICIO_EXECUCAO.isoformat()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 3] Funções Utilitárias

# COMMAND ----------

def sanitizar_snake_case(nome: str) -> str:
    """
    Converte nome de coluna para snake_case.
    Remove acentos, espaços e caracteres especiais.
    """
    nfkd       = unicodedata.normalize("NFKD", nome)
    sem_acento = "".join(c for c in nfkd if not unicodedata.combining(c))
    lower      = sem_acento.lower()
    limpo      = re.sub(r"[\s\-\.\/\\()]+", "_", lower)
    apenas_aln = re.sub(r"[^a-z0-9_]", "", limpo)
    return re.sub(r"_+", "_", apenas_aln).strip("_")


def sanitizar_colunas_df(df: DataFrame) -> DataFrame:
    """Aplica snake_case em todos os nomes de colunas."""
    vistas = []
    for col_orig in df.columns:
        col_nova = sanitizar_snake_case(col_orig)
        if col_nova in vistas:
            col_nova = col_nova + "_dup"
        vistas.append(col_nova)
        if col_orig != col_nova:
            df = df.withColumnRenamed(col_orig, col_nova)
    return df


def detectar_formato_data(df: DataFrame, sample_col: str = "dt_notific") -> str:
    """
    Detecta o formato de data presente no arquivo.
    Retorna 'ISO_DATE' (YYYY-MM-DD) ou 'ISO_DATETIME' (YYYY-MM-DDTHH:MM:SS.mmmZ).
    Usado apenas para documentação — a Bronze preserva STRING sem alterar.
    """
    sample = df.filter(F.col(sample_col).isNotNull()).limit(10)
    vals   = [r[sample_col] for r in sample.collect() if r[sample_col]]
    if not vals:
        return "DESCONHECIDO"
    # ISO 8601 com tempo: contém 'T'
    if any("T" in str(v) for v in vals):
        return "ISO_DATETIME_UTC"   # ex: 2025-10-16T00:00:00.000Z
    return "ISO_DATE"               # ex: 2025-10-16


def deduplicar_intra_batch(df: DataFrame) -> DataFrame:
    """
    Remove duplicatas dentro do mesmo batch (mesmo arquivo).
    Cross-batch é tratado pelo MERGE SCD2 na Silver.
    """
    from pyspark.sql import Window

    # Chave de deduplicação: nu_notific quando disponível,
    # fallback para hash técnico de colunas alternativas
    colunas_alt = ["dt_notific", "dt_sin_pri", "sg_uf_not",
                   "co_mun_not", "cs_sexo", "nu_idade_n"]
    cols_presentes = [c for c in colunas_alt if c in df.columns]

    exprs_hash = [
        F.coalesce(F.trim(F.col(c).cast("string")), F.lit("n/a"))
        for c in cols_presentes
    ]

    df = df.withColumn(
        "_chave_dedup",
        F.when(
            F.col("nu_notific").isNotNull() & (F.trim(F.col("nu_notific")) != ""),
            F.col("nu_notific")
        ).otherwise(F.sha2(F.concat_ws("||", *exprs_hash), 256))
    )

    w = (
        Window
        .partitionBy("_chave_dedup")
        .orderBy(F.col("dh_ingestao_bronze").asc())
    )

    df_dedup = (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn", "_chave_dedup")
    )

    removidos = df.count() - df_dedup.count()
    if removidos > 0:
        print(f"   Deduplicação intra-batch: {removidos:,} duplicatas removidas")

    return df_dedup


print(" Funções utilitárias definidas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 4] Função de Processamento do Batch
# MAGIC
# MAGIC O Auto Loader chama esta função para cada novo arquivo detectado no Volume.
# MAGIC
# MAGIC ### Como o formato de data é tratado
# MAGIC
# MAGIC ```
# MAGIC INFLUD25_DATASUS-Versao26-06-2025.csv  → "2025-01-15"          (ISO_DATE)
# MAGIC INFLUD25-2025.csv                       → "2025-01-15T00:00:00.000Z" (ISO_DATETIME_UTC)
# MAGIC INFLUD26-2026.csv                       → "2026-03-24T00:00:00.000Z" (ISO_DATETIME_UTC)
# MAGIC ```
# MAGIC
# MAGIC **Na Bronze:** todas ficam como STRING — a diferença é apenas documentada
# MAGIC em `formato_data_origem`. Sem transformação de valor.
# MAGIC
# MAGIC **Na Silver:** a função `normalizar_data()` converte ambos os formatos
# MAGIC para `date` usando `to_date()` com os dois padrões.

# COMMAND ----------

def processar_batch(df_batch: DataFrame, batch_id: int) -> None:
    """
    Processamento de cada batch de arquivos novos detectados pelo Auto Loader.
    """
    total_raw = df_batch.count()
    if total_raw == 0:
        print(f"   Batch {batch_id}: vazio — nenhuma ação.")
        return

    print(f"\n{'─'*60}")
    print(f" batch_id={batch_id} | {total_raw:,} registros | run_id={RUN_ID}")

    # ── 1. Sanitizar colunas → snake_case ──────────────────────────────────────
    df = sanitizar_colunas_df(df_batch)

    # ── 2. Detectar formato de data do arquivo ─────────────────────────────────
    # A coluna _metadata.file_name informa o nome do arquivo de cada registro
    # Usamos isso para documentar a versão e o formato sem hardcodar
    fmt_data = detectar_formato_data(df, "dt_notific")

    # ── 3. Determinar versão do arquivo por nome ───────────────────────────────
    # _metadata.file_name é uma coluna especial do Auto Loader:
    # cada registro sabe de qual arquivo CSV ele veio
    df = df.withColumn("_arquivo_nome", F.col("_metadata.file_name"))

    # Mapear nome do arquivo → versão usando SQL CASE WHEN
    versao_expr = F.when(
        F.col("_arquivo_nome").contains("Versao26-06-2025"), "26-06-2025"
    ).when(
        F.col("_arquivo_nome").contains("INFLUD25-2025"),    "2025"
    ).when(
        F.col("_arquivo_nome").contains("INFLUD26-2026"),    "2026"
    ).otherwise("desconhecida")

    # ── 4. Adicionar colunas obrigatórias de auditoria ─────────
    df = (
        df
        .withColumn("dh_ingestao_bronze",    F.current_timestamp())
        .withColumn("nome_arquivo_origem",   F.col("_metadata.file_name"))
        .withColumn("run_id",                F.lit(RUN_ID))
        .withColumn("nome_sistema_origem",   F.lit(NOME_SISTEMA_ORIGEM))
        .withColumn("versao_arquivo_origem", versao_expr)
        # Documentar formato de data — facilita o tratamento na Silver
        .withColumn("formato_data_origem",   F.lit(fmt_data))
        .drop("_arquivo_nome")
    )

    # ── 5. Deduplicação intra-batch ────────────────────────────────────────────
    df = deduplicar_intra_batch(df)

    # ── 6. Validação mínima ────────────────────────────────────────────────────
    cols_obrig = [
        "dh_ingestao_bronze", "nome_arquivo_origem",
        "run_id", "nome_sistema_origem", "versao_arquivo_origem",
        "formato_data_origem",
    ]
    for col in cols_obrig:
        assert col in df.columns, f"ERRO: Coluna obrigatória ausente: {col}"

    # ── 7. Gravar em Delta — APPEND ────────────────────────────────────────────
    (
        df.write
          .format("delta")
          .mode("append")
          .option("mergeSchema", "true")
          .saveAsTable(FULL_TABLE_NAME)
    )

    total_gravado = df.count()
    print(f"   Gravados: {total_gravado:,} | formato_data: {fmt_data}")
    print(f"   Tabela: {FULL_TABLE_NAME}")


print(" processar_batch definida.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 5] Auto Loader — Execução

# COMMAND ----------

print(f"   Iniciando Auto Loader...")
print(f"   Volume : {VOLUME_RAW_PATH}")
print(f"   Trigger: {'once' if TRIGGER_ONCE else 'availableNow'}")
print()

query = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",               "csv")
    .option("sep",                             SEPARADOR_CSV)
    .option("cloudFiles.inferColumnTypes",     "false")   # Bronze: tudo STRING
    .option("cloudFiles.schemaLocation",       SCHEMA_HINTS_PATH)
    .option("cloudFiles.schemaEvolutionMode",  "addNewColumns")
    .option("cloudFiles.includeExistingFiles", "true")
    # Habilitar leitura de metadados do arquivo (_metadata.file_name)
    .option("header",                          "true")
    .option("encoding",                        "UTF-8")
    .option("quote",                           '"')
    .option("escape",                          '"')
    .option("ignoreLeadingWhiteSpace",         "true")
    .option("ignoreTrailingWhiteSpace",        "true")
    .load(VOLUME_RAW_PATH)
    .writeStream
    .foreachBatch(processar_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
)

if TRIGGER_ONCE:
    query = query.trigger(once=True)
else:
    query = query.trigger(availableNow=True)

stream = query.start()
stream.awaitTermination()

print("\n Auto Loader concluído.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 6] Verificação Pós-Gravação

# COMMAND ----------

df_bronze = spark.table(FULL_TABLE_NAME)
total_tabela  = df_bronze.count()
total_run     = df_bronze.filter(F.col("run_id") == RUN_ID).count()

# Distribuição por arquivo e formato de data
print(" Distribuição por arquivo e formato de data:")
df_bronze.filter(F.col("run_id") == RUN_ID) \
    .groupBy("nome_arquivo_origem", "formato_data_origem", "versao_arquivo_origem") \
    .count() \
    .orderBy("nome_arquivo_origem") \
    .show(20, truncate=False)

print(f"   Total acumulado na tabela : {total_tabela:,}")
print(f"   Registros deste run_id    : {total_run:,}")
print(f"   run_id                    : {RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 7] Otimização

# COMMAND ----------

print(f" Executando OPTIMIZE...")
spark.sql(f"OPTIMIZE {FULL_TABLE_NAME}")
print(f" OPTIMIZE concluído.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 8] Saída

# COMMAND ----------

print(f"\n{'='*60}")
print(f"  BRONZE AUTO LOADER CONCLUÍDO")
print(f"  Tabela  : {FULL_TABLE_NAME}")
print(f"  run_id  : {RUN_ID}")
print(f"  Linhas  : {total_run:,}")
print(f"  Início  : {DH_INICIO_EXECUCAO.isoformat()}")
print(f"{'='*60}")

dbutils.notebook.exit(
    f"SUCESSO|run_id={RUN_ID}|linhas={total_run}"
)
