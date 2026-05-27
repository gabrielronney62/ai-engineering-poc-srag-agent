# Mapeamento Bronze → Silver

**Projeto:** PoC SRAG — HealthCare Indicium  
**Autor:** Gabriel Ronney da Silva  
**Atualizado:** 2026-05-27

---

## Fontes da Bronze SRAG

| Arquivo | Registros | Período | Formato de Data |
|---|---|---|---|
| `INFLUD25_DATASUS-Versao26-06-2025.csv` | 165.397 | Dez/2024–Jun/2025 | `YYYY-MM-DD` |
| `INFLUD25-2025.csv` | 336.239 | Dez/2024–Jan/2026 | `YYYY-MM-DDTHH:MM:SS.mmmZ` |
| `INFLUD26-2026.csv` | 101.425 | Jan–Mai/2026 | `YYYY-MM-DDTHH:MM:SS.mmmZ` |

**Total único após SCD2 na Silver: ~440.674 registros**

### Normalização de Datas

A Bronze preserva os valores como STRING.
A Silver normaliza para `DateType` via:

```python
F.expr("try_to_date(substring(col, 1, 10), 'yyyy-MM-dd')")
# Extrai os 10 primeiros caracteres — funciona para ambos os formatos
```

---

## Pseudoanonimização — o que muda por campo

### Campos cujo valor original vai apenas para o cofre PII

**PII direto (15 campos):**
`nu_cpf`, `nu_cns`, `nm_pacient`, `nm_mae_pac`, `nu_cep`, `nm_bairro`,
`nm_logrado`, `nu_numero`, `nm_complem`, `nu_ddd_tel`, `nu_telefon`,
`observa`, `nome_prof`, `reg_prof`, `vg_prof`

→ Silver recebe coluna `{campo}_hash` com hash SHA-256 visível

**Quasi-identificador de alto risco:**
`dt_nasc` — Silver recebe `dt_nasc_hash`. Substitutos: `faixa_etaria` e `idade_anos_calculada`.

### Campos excluídos sem hash (sem valor analítico)

`lote_1_cov`, `lote_2_cov`, `lote_ref`, `lote_ref2`, `lote_adic`, `lot_re_bi`,
`fab_cov1`, `fab_cov2`, `fab_covrf`, `fab_covrf2`, `fab_adic`, `fab_re_bi`
— lotes e fabricantes de vacinas COVID

---

## Mapeamento por Grupo

### A — Identificação e Rastreabilidade

| Bronze → Silver | Tipo | Regra |
|---|---|---|
| `nu_notific` → `nu_notific` | string | Chave natural |
| — → `hash_caso` | string | `SHA-256("srag_poc_salt_v1" \|\| nu_notific)` |
| `dt_notific` → `dt_notific` | date | try_to_date(substring, 10) |
| `dt_sin_pri` → `dt_sin_pri` | date | Eixo temporal principal |
| `dt_digita` → `dt_digita` | date | Watermark |
| `sem_not` → `sem_not` | string | Semana epidemiológica notificação |
| `sem_pri` → `sem_pri` | string | Semana epidemiológica sintomas |

### B — Localidade

| Bronze → Silver | Tipo | Regra |
|---|---|---|
| `sg_uf_not` → `sg_uf_not` | string | UPPER |
| `sg_uf` → `sg_uf_residencia` | string | UPPER (renomeado para clareza) |
| `co_mun_not` → `co_mun_not` | string | — |
| `co_mun_res` → `co_mun_residencia` | string | — |
| `sg_uf_inte` → `sg_uf_internacao` | string | UPPER |
| `co_mu_inte` → `co_mun_internacao` | string | — |
| `cs_zona` → `cs_zona` | integer | 1=Urbana, 2=Rural, 3=Periurbana |

### C — Perfil Demográfico

| Bronze → Silver | Tipo | Regra |
|---|---|---|
| `cs_sexo` → `cs_sexo` | string | UPPER (M, F, I) |
| `nu_idade_n` + `tp_idade` → `idade_anos_calculada` | decimal | TP=3: anos; TP=2: meses/12; TP=1: dias/365 |
| — → `faixa_etaria` | string | 00-04, 05-11, 12-17, 18-29, 30-39, 40-49, 50-59, 60-69, 70-79, 80+ |
| `cs_gestant` → `cs_gestant` | integer | 1-6, 9 |
| `cs_raca` → `cs_raca` | integer | 1=Branca...5=Indígena, 9=Ignorado |
| `cs_escol_n` → `cs_escol_n` | integer | 0-4, 5=N/A, 9=Ignorado |

### D — Sintomas Clínicos

`febre`, `tosse`, `garganta`, `dispneia`, `desc_resp`, `saturacao`,
`diarreia`, `vomito`, `dor_abd`, `fadiga`, `perd_olft`, `perd_pala` → integer (1=Sim, 2=Não, 9=Ignorado)

### E — Fatores de Risco

`fator_risc`, `cardiopati`, `asma`, `diabetes`, `neurologic`, `pneumopati`,
`imunodepre`, `renal`, `obesidade`, `tabag` → integer (1=Sim, 2=Não, 9=Ignorado)

### F — Vacinação

| Bronze → Silver | Tipo | Regra |
|---|---|---|
| `vacina` → `vacina_gripe` | integer | 1=Sim, 2=Não, 9=Ignorado |
| `vacina_cov` → `vacina_covid` | integer | 1=Sim, 2=Não, 9=Ignorado |
| `dose_1_cov` a `dos_re_bi` → idem | date | try_to_date(substring, 10) |
| — → `qtd_doses_covid_registradas` | integer | Count de doses não-nulas |

### G — Internação e UTI

| Bronze → Silver | Tipo | Regra |
|---|---|---|
| `hospital` → `hospital` | integer | 1=Sim, 2=Não, 9=Ignorado |
| `dt_interna` → `dt_interna` | date | try_to_date(substring, 10) |
| `uti` → `uti` | integer | 1=Sim, 2=Não, 9=Ignorado |
| `dt_entuti` → `dt_entuti` | date | try_to_date(substring, 10) |
| `dt_saiduti` → `dt_saiduti` | date | try_to_date(substring, 10) |
| — → `dias_uti` | integer | `datediff(dt_saiduti, dt_entuti)` |
| `suport_ven` → `suport_ven` | integer | 1=Invasivo, 2=Não invasivo, 3=Não |

### H — Diagnóstico Laboratorial

`pcr_resul`, `pos_pcrflu`, `tp_flu_pcr`, `pcr_sars2`, `pcr_vsr`, `pcr_rino`,
`res_an`, `an_sars2`, `an_vsr`, `classi_fin`, `criterio` → integer

### I — Evolução

| Bronze → Silver | Tipo | Regra |
|---|---|---|
| `evolucao` → `evolucao` | integer | 1=Cura, 2=Óbito, 3=Óbito outras causas, 9=Ignorado |
| `dt_evoluca` → `dt_evoluca` | date | try_to_date(substring, 10) |
| `dt_encerra` → `dt_encerra` | date | try_to_date(substring, 10) |

### J — 17 Flags Epidemiológicas

| Flag | Regra |
|---|---|
| `flag_caso_srag` | `1` (literal — toda linha é um caso) |
| `flag_obito_srag` | `evolucao = 2` |
| `flag_obito_outras_causas` | `evolucao = 3` |
| `flag_cura` | `evolucao = 1` |
| `flag_evolucao_informada` | `evolucao IN (1,2,3)` — denominador da taxa de mortalidade |
| `flag_hospitalizado` | `hospital = 1 OR dt_interna IS NOT NULL` |
| `flag_uti` | `uti = 1` — numerador da taxa de UTI |
| `flag_uso_suporte_ventilatorio` | `suport_ven IN (1,2)` |
| `flag_vacinado_gripe` | `vacina_gripe = 1` |
| `flag_info_vacina_gripe` | `vacina_gripe IN (1,2)` — denominador vacinação gripe |
| `flag_vacinado_covid` | `vacina_covid = 1` |
| `flag_info_vacina_covid` | `vacina_covid IN (1,2)` — denominador vacinação COVID |
| `flag_covid_classificacao_final` | `classi_fin = 5` |
| `flag_influenza_classificacao_final` | `classi_fin = 1` |
| `flag_outro_virus_classificacao_final` | `classi_fin = 2` |
| `flag_srag_nao_especificado` | `classi_fin = 4` |

### K — Colunas Técnicas

| Coluna | Tipo | Regra |
|---|---|---|
| `hash_registros` | string | SHA-256 de todas as colunas de negócio — detecta mudanças para SCD2 |
| `dh_ultima_atualizacao` | timestamp | `COALESCE(dt_digita, dt_encerra, dt_notific, dh_ingestao_bronze)` |
| `dh_ingestao_silver` | timestamp | `current_timestamp()` |
| `registro_mais_atual` | boolean | true = versão vigente |
| `dh_inicio_vigencia` | timestamp | Quando esta versão ficou ativa |
| `dh_fim_vigencia` | timestamp | Quando foi substituída (null = ainda vigente) |
| `run_id` | string | UUID da execução da Silver |
| `nome_arquivo_origem` | string | Nome do CSV de origem |

---

## Mapeamento Leitos CNES — Bronze → Silver

### Colunas Excluídas (PII do estabelecimento)

`no_logradouro`, `nu_endereco`, `no_complemento`, `no_bairro`, `co_cep`,
`nu_telefone`, `no_email`, `razao_social`, `motivo_desabilitacao`

### Principais Colunas Mantidas + Sanitização

| Bronze | Silver | Tipo | Tratamento |
|---|---|---|---|
| `COMP` | `comp` → `ano_mes`, `ano`, `mes` | int → string | Derivar partes da competência |
| `REGIAO` | `regiao` | string | UPPER |
| `UF` | `uf` | string | UPPER |
| `CO_IBGE` | `co_ibge` | string | NULL nos dados 2025 (só 2026) |
| `MUNICIPIO` | `municipio` | string | Title Case |
| `NOME_ESTABELECIMENTO` | `nome_estabelecimento` | string | Title Case |
| `UTI_TOTAL_EXIST` | `uti_total_exist` | integer | Denominador da taxa de UTI |
| `LEITOS_EXISTENTES` | `leitos_existentes` | integer | — |

### Tabela Agregada: `silver_cnes_leitos_uti_por_uf`

Grão: **1 linha por UF + ano_mes** — denominador real para a taxa de ocupação de UTI

```sql
taxa_uso_uti_real =
    SUM(flag_uti) para SRAG por UF/mês
    / uti_total_exist por UF/mês (da tabela CNES)
```
