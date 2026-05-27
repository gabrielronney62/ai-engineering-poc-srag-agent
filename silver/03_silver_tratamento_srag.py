# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver: Tratamento, Pseudoanonimização e SCD2 — SRAG DATASUS
# MAGIC
# MAGIC **PoC:** HealthCare Gabriel Ronney — Monitoramento de SRAG
# MAGIC **Camada:** Silver
# MAGIC **Estratégia:** MERGE SCD Tipo 2 (Playbook §1.2)
# MAGIC
# MAGIC ---
# MAGIC ### O que este notebook resolve: normalização de formatos de data
# MAGIC
# MAGIC Os 3 arquivos CSV têm formatos de data diferentes:
# MAGIC
# MAGIC | Fonte | Formato | Exemplo |
# MAGIC |---|---|---|
# MAGIC | Original (Versao26-06-2025) | `ISO_DATE` | `2025-01-15` |
# MAGIC | INFLUD25-2025.csv | `ISO_DATETIME_UTC` | `2025-01-15T00:00:00.000Z` |
# MAGIC | INFLUD26-2026.csv | `ISO_DATETIME_UTC` | `2026-03-24T00:00:00.000Z` |
# MAGIC
# MAGIC A função `normalizar_data()` usa `COALESCE(to_date(col, 'yyyy-MM-dd'), to_date(col, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))`
# MAGIC para converter ambos os formatos para `DateType` de forma transparente.
# MAGIC
# MAGIC **Resultado:** A Silver tem sempre `DateType` independente do arquivo de origem.

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 1] Setup

# COMMAND ----------

import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType, DecimalType
from pyspark.sql import Window
from delta.tables import DeltaTable

RUN_ID              = str(uuid.uuid4())
DH_INICIO_EXECUCAO  = datetime.now(timezone.utc)
SALT_PII            = dbutils.secrets.get(scope="poc_srag", key="salt_pii")

# ── Tabelas ────────────────────────────────────────────────────────────────────
CATALOGO      = "certificacao_indicium"
SCHEMA        = "poc_srag_datasus"
FULL_BRONZE   = f"{CATALOGO}.{SCHEMA}.bronze_poc_srag_datasus"
FULL_SILVER   = f"{CATALOGO}.{SCHEMA}.silver_poc_srag_datasus"
FULL_MAPA_PII = f"{CATALOGO}.auditoria_pii.mapa_identificacao_dados_srag_datasus"

print(f" run_id  : {RUN_ID}")
print(f" Bronze  : {FULL_BRONZE}")
print(f" Silver  : {FULL_SILVER}")
print(f" Mapa PII: {FULL_MAPA_PII}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 2] Função de Normalização de Datas
# MAGIC
# MAGIC Resolve a diferença de formato entre os arquivos de origem.
# MAGIC Aplica a mesma lógica para todas as 25 colunas de data.

# COMMAND ----------

def normalizar_data(col_name: str) -> F.Column:
    """
    Normaliza coluna de data para DateType.

    Suporta dois formatos:
      - 'yyyy-MM-dd'                    → ex: 2025-01-15      (arquivo original)
      - "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" → ex: 2025-01-15T00:00:00.000Z (novos)

    Usa COALESCE: tenta o primeiro formato, se falhar tenta o segundo.
    Se ambos falharem (valor inválido ou nulo), retorna NULL.
    """
    # try_to_date: tolerante ao ANSI estrito do Photon engine (DBR 13+)
    # F.to_date lanca excecao quando o formato nao bate — try_to_date retorna NULL
    col_ref = f"`{col_name}`"
    return F.coalesce(
        F.expr(f"try_to_date({col_ref}, 'yyyy-MM-dd')"),
        F.expr(f"""try_to_date({col_ref}, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")"""),
        F.expr(f"""try_to_date({col_ref}, "yyyy-MM-dd'T'HH:mm:ss'Z'")"""),
        F.expr(f"try_to_date({col_ref}, 'yyyyMMdd')"),
    )


# Todas as 25 colunas de data do SIVEP-Gripe
COLUNAS_DATA = [
    "dt_notific", "dt_sin_pri", "dt_nasc",
    "dt_ut_dose", "dt_vac_mae", "dt_doseuni", "dt_1_dose", "dt_2_dose",
    "dt_antivir", "dt_interna", "dt_entuti",  "dt_saiduti",
    "dt_raiox",   "dt_coleta",  "dt_pcr",     "dt_evoluca", "dt_encerra",
    "dt_digita",  "dt_vgm",     "dt_rt_vgm",  "dt_tomo",    "dt_res_an",
    "dt_co_sor",  "dt_res",     "dt_trt_cov",
]

print(f" normalizar_data() definida para {len(COLUNAS_DATA)} colunas de data.")
print(f" Formatos suportados: ISO_DATE e ISO_DATETIME_UTC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 3] Leitura da Bronze e Seleção de Colunas

# COMMAND ----------

df_bronze = spark.table(FULL_BRONZE)

# Identificar run mais recente da Bronze para processamento incremental
run_recente = (
    df_bronze
    .orderBy(F.col("dh_ingestao_bronze").desc())
    .limit(1)
    .first()["run_id"]
)

# Processar lote mais recente (run mais novo não processado ainda)
# Em produção usar watermark; para PoC usamos o run mais recente
df_lote = df_bronze.filter(F.col("run_id") == run_recente)
total_bronze = df_lote.count()

print(f"   Lote para processamento:")
print(f"   run_id   : {run_recente}")
print(f"   Registros: {total_bronze:,}")

# Distribuição por arquivo de origem
df_lote.groupBy("nome_arquivo_origem", "formato_data_origem").count().show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 4] Identificação e Separação de PII
# MAGIC
# MAGIC **ATENÇÃO:** Esta célula captura PII diretamente do `df_lote` (Bronze),
# MAGIC ANTES de qualquer seleção. Se ocorresse após, as colunas já estariam perdidas.

# COMMAND ----------

# ── Categoria 1: PII Direto ───────────────────────────────────────────────────
# Identificam a pessoa diretamente. LGPD Art. 5 inciso I.
# Tratamento: valor original → cofre PII (hash + texto claro)
#             Silver recebe APENAS o hash como coluna (ex: nu_cpf_hash)
#             O valor em texto claro nunca aparece na Silver.
COLUNAS_PII_DIRETO = [
    "nu_cpf",     # CPF — identificador único direto
    "nu_cns",     # Cartao Nacional de Saude
    "nm_pacient", # Nome completo do paciente
    "nm_mae_pac", # Nome da mae
    "nu_cep",     # CEP de residencia
    "nm_bairro",  # Bairro de residencia
    "nm_logrado", # Logradouro
    "nu_numero",  # Numero do endereco
    "nm_complem", # Complemento
    "nu_ddd_tel", # DDD do telefone
    "nu_telefon", # Numero do telefone
    "observa",    # Campo livre — pode conter PII implicito
    "nome_prof",  # Nome do profissional notificante
    "reg_prof",   # Registro profissional (CRM/COREN)
    "vg_prof",    # Profissional de vigilancia genomica
]

# ── Categoria 2: Quasi-identificadores de Alto Risco ─────────────────────────
# Combinados com sg_uf + cs_sexo na Silver: re-id de ~87% dos casos.
# Ref: Sweeney (2002). k-anonymity: A Model for Protecting Privacy.
# Tratamento: valor original → cofre PII (hash + texto claro)
#             Silver recebe APENAS o hash como coluna (ex: nu_cpf_hash)
#             O valor em texto claro nunca aparece na Silver.
# Substituto analitico: faixa_etaria + idade_anos_calculada (ja na Silver).
COLUNAS_QUASI_ALTO_RISCO = [
    "dt_nasc",  # Data de nascimento — quasi-id alto risco
               # Silver recebe: dt_nasc_hash (coluna com hash visível)
               # Valor original: somente no cofre auditoria_pii
               # Combinado com sg_uf + cs_sexo: ~87% re-id (Sweeney 2002)
]

# ── Categoria 3: Quasi-identificadores de Baixo Risco (MANTIDOS) ─────────────
# Necessarios para analise epidemiologica geografica. Baixo risco isolado.
# Documentados como quasi-id: docs/decisao_arquitetural_pii_poc.md
COLUNAS_QUASI_BAIXO_RISCO_MANTIDAS = {
    "id_mn_resi": "Municipio residencia — analise geografica",
    "id_regiona": "Regional de saude de notificacao",
    "id_rg_resi": "Regional de saude de residencia",
    "nm_un_inte": "Nome do hospital de internacao",
    "morb_desc":  "Descricao de comorbidades (texto livre)",
    "outro_des":  "Descricao de outros sintomas (texto livre)",
    "tem_cpf":    "Indicador de presenca de CPF (metadado)",
}

# Quais colunas realmente existem no lote
cols_pii_direto_presentes       = [c for c in COLUNAS_PII_DIRETO       if c in df_lote.columns]
cols_quasi_alto_risco_presentes = [c for c in COLUNAS_QUASI_ALTO_RISCO if c in df_lote.columns]
cols_para_mapa_presentes        = cols_pii_direto_presentes + cols_quasi_alto_risco_presentes
cols_pii_presentes              = cols_para_mapa_presentes  # alias

print(f"PII Direto encontrado      : {len(cols_pii_direto_presentes)} colunas")
print(f"Quasi-id Alto Risco        : {len(cols_quasi_alto_risco_presentes)} colunas")
print(f"Total para mapa PII        : {len(cols_para_mapa_presentes)} colunas")

if len(cols_para_mapa_presentes) == 0:
    raise RuntimeError(
        "ERRO CRITICO: Nenhuma coluna PII encontrada em df_lote. "
        "Esta celula deve executar ANTES da selecao de colunas."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 5] Pseudoanonimização e Mapa PII

# COMMAND ----------

def gerar_hash(col: F.Column) -> F.Column:
    """SHA-256 determinístico com salt. Idêntico valor → idêntico hash (permite JOINs)."""
    return F.sha2(
        F.concat_ws("||", F.lit(SALT_PII), F.coalesce(col.cast("string"), F.lit(""))),
        256
    )


# Gerar hash_caso a partir de nu_notific
df_com_hash = df_lote.withColumn(
    "hash_caso",
    gerar_hash(F.coalesce(F.col("nu_notific"), F.lit("SEM_NOTIFIC")))
)

# Mapa PII: hash_caso (chave) + pares {campo_hash + campo_original} para cada PII
cols_mapa = {
    "hash_caso":  F.col("hash_caso"),
    "nu_notific": F.col("nu_notific"),
}
for col_pii in cols_para_mapa_presentes:
    cols_mapa[f"{col_pii}_hash"] = gerar_hash(F.col(col_pii))  # referencia na Silver
    cols_mapa[col_pii]           = F.col(col_pii)               # texto claro no cofre

cols_mapa["origem_sistema"]      = F.lit("Open DATASUS - SIVEP-Gripe")
cols_mapa["nome_arquivo_origem"] = F.col("nome_arquivo_origem")
cols_mapa["run_id"]              = F.lit(RUN_ID)
cols_mapa["dh_carga_pii"]        = F.current_timestamp()

df_mapa_pii = (
    df_com_hash
    .select([expr.alias(name) for name, expr in cols_mapa.items()])
    .distinct()
)
total_mapa = df_mapa_pii.count()

print(f"Mapa PII: {total_mapa:,} registros | {len(df_mapa_pii.columns)} colunas")
print(f"Campos com hash+texto: {len(cols_para_mapa_presentes)}")

(
    df_mapa_pii.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(FULL_MAPA_PII)
)
assert total_mapa > 0, "ERRO: Mapa PII vazio — verificar ordem das celulas"
print(f"OK Mapa PII gravado: {FULL_MAPA_PII}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 6] Transformações Silver
# MAGIC
# MAGIC Aplica tipagem, normalização de datas, cálculos derivados e 17 flags.

# COMMAND ----------

# Colunas a excluir da Silver
# - PII direto (nu_cpf, nm_pacient, etc.):
#     valor original → SOMENTE no cofre PII
#     Silver recebe: nu_cpf_hash, nm_pacient_hash, ... (colunas com hash)
# - Quasi-id alto risco (dt_nasc):
#     valor original → SOMENTE no cofre PII
#     Silver recebe: dt_nasc_hash (coluna com o hash visível)
# - Lotes/fabricantes de vacina: sem valor epidemiologico analitico
COLUNAS_EXCLUIR = set(
    COLUNAS_PII_DIRETO +
    COLUNAS_QUASI_ALTO_RISCO +
    [
    "lote_1_cov", "lote_2_cov", "lote_ref", "lote_ref2", "lote_adic", "lot_re_bi",
    "fab_cov1",  "fab_cov2",  "fab_covrf", "fab_covrf2", "fab_adic", "fab_re_bi",
    "fab_cov_1", "fab_cov_2",  # variante com underscore nos CSVs novos
    "formato_data_origem",
    ]
)

# Para cada quasi-id de alto risco:
#   1. Criar coluna {campo}_hash na Silver (hash visível para quem consulta)
#   2. Excluir coluna {campo} original da Silver (valor vai somente para o cofre)
# Resultado: quem vê a Silver encontra dt_nasc_hash — hash determinístico,
# não o valor real. Para obter o valor real: JOIN com auditoria_pii.
df_com_hashes_quasi = df_com_hash
for col_q in cols_quasi_alto_risco_presentes:
    # dt_nasc → adiciona dt_nasc_hash à Silver antes de remover dt_nasc
    df_com_hashes_quasi = df_com_hashes_quasi.withColumn(
        f"{col_q}_hash", gerar_hash(F.col(col_q))
    )

# Selecionar colunas da Silver (sem PII direto nem quasi-id alto risco)
cols_silver = [c for c in df_com_hashes_quasi.columns if c not in COLUNAS_EXCLUIR]
df_sel = df_com_hashes_quasi.select(cols_silver)

dt_ok = "dt_nasc" not in df_sel.columns
dt_hash_ok = "dt_nasc_hash" in df_sel.columns if cols_quasi_alto_risco_presentes else True
print(f"Silver: {len(df_sel.columns)} colunas")
print(f"  dt_nasc (valor original) na Silver: {not dt_ok} — deve ser False")
print(f"  dt_nasc_hash (hash visível) na Silver: {dt_hash_ok} — deve ser True")
print()
print("  Colunas de hash visiveis na Silver:")
hash_cols = [col for col in df_sel.columns if col.endswith("_hash")]
for hc in hash_cols:
    print(f"    {hc}")

# ── Tipagem de inteiros ────────────────────────────────────────────────────────
COLUNAS_INT = [
    "nu_idade_n", "tp_idade", "cs_gestant", "cs_raca", "cs_escol_n", "cs_zona",
    "febre", "tosse", "garganta", "dispneia", "desc_resp", "saturacao",
    "diarreia", "vomito", "dor_abd", "fadiga", "perd_olft", "perd_pala",
    "fator_risc", "cardiopati", "asma", "diabetes", "neurologic", "pneumopati",
    "imunodepre", "renal", "obesidade", "tabag",
    "vacina", "vacina_cov",
    "hospital", "uti", "suport_ven",
    "amostra", "pcr_resul", "pos_pcrflu", "tp_flu_pcr",
    "classi_fin", "criterio", "evolucao",
    "res_an",
]
for c in COLUNAS_INT:
    if c in df_sel.columns:
        df_sel = df_sel.withColumn(c, F.col(c).cast(IntegerType()))

# ── Normalização de TODAS as datas (resolve ISO_DATE e ISO_DATETIME_UTC) ───────
for c in COLUNAS_DATA:
    if c in df_sel.columns:
        df_sel = df_sel.withColumn(c, normalizar_data(c))

# ── Padronização de texto ──────────────────────────────────────────────────────
if "sg_uf_not" in df_sel.columns:
    df_sel = df_sel.withColumn("sg_uf_not", F.upper(F.trim(F.col("sg_uf_not"))))
if "sg_uf" in df_sel.columns:
    df_sel = df_sel.withColumn("sg_uf_residencia",
        F.upper(F.trim(F.col("sg_uf")))).drop("sg_uf")
if "cs_sexo" in df_sel.columns:
    df_sel = df_sel.withColumn("cs_sexo", F.upper(F.trim(F.col("cs_sexo"))))

# ── Campos calculados ──────────────────────────────────────────────────────────
# Idade em anos normalizados
df_sel = df_sel.withColumn(
    "idade_anos_calculada",
    F.when(F.col("tp_idade") == 3, F.col("nu_idade_n").cast("decimal(5,1)"))
     .when(F.col("tp_idade") == 2, F.round(F.col("nu_idade_n") / 12.0, 1))
     .when(F.col("tp_idade") == 1, F.round(F.col("nu_idade_n") / 365.0, 1))
     .otherwise(F.lit(None))
)

# Faixa etária
df_sel = df_sel.withColumn(
    "faixa_etaria",
    F.when(F.col("idade_anos_calculada") <  5,  F.lit("00-04"))
     .when(F.col("idade_anos_calculada") < 12,  F.lit("05-11"))
     .when(F.col("idade_anos_calculada") < 18,  F.lit("12-17"))
     .when(F.col("idade_anos_calculada") < 30,  F.lit("18-29"))
     .when(F.col("idade_anos_calculada") < 40,  F.lit("30-39"))
     .when(F.col("idade_anos_calculada") < 50,  F.lit("40-49"))
     .when(F.col("idade_anos_calculada") < 60,  F.lit("50-59"))
     .when(F.col("idade_anos_calculada") < 70,  F.lit("60-69"))
     .when(F.col("idade_anos_calculada") < 80,  F.lit("70-79"))
     .when(F.col("idade_anos_calculada").isNotNull(), F.lit("80+"))
     .otherwise(F.lit("Ignorado"))
)

# Dias em UTI
df_sel = df_sel.withColumn(
    "dias_uti",
    F.when(
        F.col("dt_entuti").isNotNull() & F.col("dt_saiduti").isNotNull(),
        F.datediff(F.col("dt_saiduti"), F.col("dt_entuti"))
    ).otherwise(F.lit(None))
)

# Quantidade de doses COVID registradas
doses_cols = ["dose_1_cov","dose_2_cov","dose_ref","dose_2ref","dose_adic","dos_re_bi"]
doses_presentes = [c for c in doses_cols if c in df_sel.columns]
if doses_presentes:
    df_sel = df_sel.withColumn(
        "qtd_doses_covid_registradas",
        sum(F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in doses_presentes)
    )

# ── 17 Flags epidemiológicas ───────────────────────────────────────────────────
df_sel = (
    df_sel
    .withColumn("flag_caso_srag",                    F.lit(1))
    .withColumn("flag_obito_srag",                   F.when(F.col("evolucao") == 2, 1).otherwise(0))
    .withColumn("flag_obito_outras_causas",          F.when(F.col("evolucao") == 3, 1).otherwise(0))
    .withColumn("flag_cura",                         F.when(F.col("evolucao") == 1, 1).otherwise(0))
    .withColumn("flag_evolucao_informada",           F.when(F.col("evolucao").isin(1,2,3), 1).otherwise(0))
    .withColumn("flag_hospitalizado",
        F.when(F.col("hospital") == 1, 1)
         .when(F.col("dt_interna").isNotNull(), 1).otherwise(0))
    .withColumn("flag_uti",                          F.when(F.col("uti") == 1, 1).otherwise(0))
    .withColumn("flag_uso_suporte_ventilatorio",     F.when(F.col("suport_ven").isin(1,2), 1).otherwise(0))
    .withColumn("flag_vacinado_gripe",               F.when(F.col("vacina") == 1, 1).otherwise(0))
    .withColumn("flag_info_vacina_gripe",            F.when(F.col("vacina").isin(1,2), 1).otherwise(0))
    .withColumn("flag_vacinado_covid",               F.when(F.col("vacina_cov") == 1, 1).otherwise(0))
    .withColumn("flag_info_vacina_covid",            F.when(F.col("vacina_cov").isin(1,2), 1).otherwise(0))
    .withColumn("flag_covid_classificacao_final",    F.when(F.col("classi_fin") == 5, 1).otherwise(0))
    .withColumn("flag_influenza_classificacao_final",F.when(F.col("classi_fin") == 1, 1).otherwise(0))
    .withColumn("flag_outro_virus_classificacao_final",F.when(F.col("classi_fin") == 2, 1).otherwise(0))
    .withColumn("flag_srag_nao_especificado",        F.when(F.col("classi_fin") == 4, 1).otherwise(0))
)

print(f" Transformações aplicadas. Total: {df_sel.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 7] Colunas Técnicas Silver + Deduplicação (Playbook §4.2)

# COMMAND ----------

# ── Watermark: dh_ultima_atualizacao ──────────────────────────────────────────
df_sel = df_sel.withColumn(
    "dh_ultima_atualizacao",
    F.coalesce(
        F.col("dt_digita").cast("timestamp"),
        F.col("dt_encerra").cast("timestamp"),
        F.col("dt_notific").cast("timestamp"),
        F.col("dh_ingestao_bronze"),
    )
)

# ── Hash de registro para deduplicação (Playbook §4.2) ────────────────────────
# Exclui colunas de metadados técnicos do hash (mudam a cada ingestão)
COLUNAS_EXCLUIR_HASH = {
    "dh_ingestao_bronze", "nome_arquivo_origem", "run_id",
    "nome_sistema_origem", "versao_arquivo_origem",
    "dh_ultima_atualizacao", "hash_registros",
    "dh_ingestao_silver", "registro_mais_atual",
    "dh_inicio_vigencia", "dh_fim_vigencia", "hash_caso",
}
cols_hash = [c for c in df_sel.columns if c not in COLUNAS_EXCLUIR_HASH]
exprs_hash = [
    F.coalesce(F.trim(F.col(c).cast("string")), F.lit("n/a"))
    for c in cols_hash
]
df_sel = df_sel.withColumn(
    "hash_registros",
    F.sha2(F.concat_ws("||", *exprs_hash), 256)
)

# ── Chave de deduplicação cross-batch ─────────────────────────────────────────
colunas_alt_hash = ["dt_notific", "dt_sin_pri", "sg_uf_not", "co_mun_not", "cs_sexo", "nu_idade_n"]
cols_alt = [c for c in colunas_alt_hash if c in df_sel.columns]
exprs_chave_alt = [F.coalesce(F.trim(F.col(c).cast("string")), F.lit("n/a")) for c in cols_alt]

df_sel = df_sel.withColumn(
    "_chave_natural",
    F.when(
        F.col("nu_notific").isNotNull() & (F.trim(F.col("nu_notific").cast("string")) != ""),
        F.col("nu_notific").cast("string")
    ).otherwise(F.sha2(F.concat_ws("||", *exprs_chave_alt), 256))
)

# Deduplicação: para mesma chave natural + mesmo hash, manter o mais recente
w_dedup = (
    Window
    .partitionBy("_chave_natural", "hash_registros")
    .orderBy(F.col("dh_ingestao_bronze").desc())
)
df_dedup = (
    df_sel
    .withColumn("_rn_dedup", F.row_number().over(w_dedup))
    .filter(F.col("_rn_dedup") == 1)
    .drop("_rn_dedup", "_chave_natural")
)

print(f" Deduplicação cross-batch: {df_sel.count():,} → {df_dedup.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 8] SCD Tipo 2 — MERGE na Silver

# COMMAND ----------

df_silver_final = df_dedup.withColumn("dh_ingestao_silver", F.current_timestamp())

if spark.catalog.tableExists(FULL_SILVER):
    delta_silver = DeltaTable.forName(spark, FULL_SILVER)

    merge_condition = "target.hash_caso = source.hash_caso"

    (
        delta_silver.alias("target")
        .merge(df_silver_final.alias("source"), merge_condition)
        # Registro mudou (hash diferente) → fechar o atual
        .whenMatchedUpdate(
            condition="target.hash_registros != source.hash_registros AND target.registro_mais_atual = true",
            set={
                "registro_mais_atual": F.lit(False),
                "dh_fim_vigencia":     F.current_timestamp(),
            }
        )
        # Registro novo → inserir
        .whenNotMatchedInsert(
            values={
                **{c: F.col(f"source.{c}") for c in df_silver_final.columns},
                "registro_mais_atual": F.lit(True),
                "dh_inicio_vigencia":  F.current_timestamp(),
                "dh_fim_vigencia":     F.lit(None).cast("timestamp"),
                "run_id":              F.lit(RUN_ID),
            }
        )
        .execute()
    )
    print(" MERGE SCD2 executado.")
else:
    # Primeira carga — criar a tabela
    df_silver_final = (
        df_silver_final
        .withColumn("registro_mais_atual", F.lit(True))
        .withColumn("dh_inicio_vigencia",  F.current_timestamp())
        .withColumn("dh_fim_vigencia",     F.lit(None).cast("timestamp"))
    )
    df_silver_final.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(FULL_SILVER)
    print(f" Silver criada (primeira carga): {df_silver_final.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 9] Verificação e Estatísticas

# COMMAND ----------

df_silver_check = spark.table(FULL_SILVER).filter(F.col("registro_mais_atual") == True)
total_ativos = df_silver_check.count()

print(f" Silver — registros ativos (registro_mais_atual=True): {total_ativos:,}")
print()

# Distribuição por período — confirmar que os 3 arquivos foram integrados
print("Distribuição por ano de DT_SIN_PRI:")
df_silver_check.withColumn("ano", F.year("dt_sin_pri")) \
    .groupBy("ano").count().orderBy("ano").show()

print("Distribuição por versao_arquivo_origem:")
df_silver_check.groupBy("versao_arquivo_origem").count().orderBy("versao_arquivo_origem").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 10] Otimização

# COMMAND ----------

print(f"  Executando OPTIMIZE + Z-ORDER...")
spark.sql(f"""
    OPTIMIZE {FULL_SILVER}
    ZORDER BY (dt_sin_pri)
""")
print(f" OPTIMIZE + Z-ORDER concluídos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 11] Saída

# COMMAND ----------

print(f"\n{'='*60}")
print(f"  SILVER CONCLUÍDA")
print(f"  Tabela  : {FULL_SILVER}")
print(f"  run_id  : {RUN_ID}")
print(f"  Ativos  : {total_ativos:,}")
print(f"  Início  : {DH_INICIO_EXECUCAO.isoformat()}")
print(f"{'='*60}")

dbutils.notebook.exit(
    f"SUCESSO|run_id={RUN_ID}|silver_ativos={total_ativos}"
)
