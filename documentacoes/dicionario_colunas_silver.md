# Dicionário de Colunas — Camada Silver

**Projeto:** PoC SRAG — HealthCare Indicium  
**Autor:** Gabriel Ronney da Silva  
**Atualizado:** 2026-05-27

---

## 1. `silver_poc_srag_datasus`

Grão: 1 linha por caso SRAG por versão.
`registro_mais_atual = true` identifica a versão vigente (SCD Tipo 2).

### Identificação e Rastreabilidade

| Coluna | Tipo | Domínio | Descrição |
|---|---|---|---|
| `nu_notific` | string | — | Número do registro no SIVEP-Gripe |
| `hash_caso` | string | SHA-256 | Identificador pseudoanonimizado do caso |
| `dt_notific` | date | — | Data de preenchimento da ficha |
| `sem_not` | string | — | Semana epidemiológica da notificação |
| `dt_sin_pri` | date | — | Data dos primeiros sintomas — eixo temporal principal |
| `sem_pri` | string | — | Semana epidemiológica dos primeiros sintomas |
| `dt_digita` | date | — | Data de digitação no sistema — watermark |

### Localidade

| Coluna | Tipo | Domínio | Descrição |
|---|---|---|---|
| `sg_uf_not` | string | UF IBGE | UF da unidade notificadora |
| `co_mun_not` | string | IBGE 6 dígitos | Município notificador |
| `sg_uf_residencia` | string | UF IBGE | UF de residência do paciente |
| `co_mun_residencia` | string | IBGE 6 dígitos | Município de residência |
| `sg_uf_internacao` | string | UF IBGE | UF de internação |
| `co_mun_internacao` | string | IBGE 6 dígitos | Município de internação |
| `cs_zona` | integer | 1=Urbana, 2=Rural, 3=Periurbana, 9=Ignorado | Zona geográfica |

### Perfil Demográfico

| Coluna | Tipo | Domínio | Descrição |
|---|---|---|---|
| `cs_sexo` | string | M, F, I | Sexo do paciente |
| `nu_idade_n` | integer | 0–150 | Idade informada |
| `tp_idade` | integer | 1=Dia, 2=Mês, 3=Ano | Unidade da idade |
| `idade_anos_calculada` | decimal | — | Idade normalizada em anos |
| `faixa_etaria` | string | 00-04...80+ | Faixa etária calculada |
| `dt_nasc_hash` | string | SHA-256 | Hash da data de nascimento — valor real apenas no cofre PII |
| `cs_gestant` | integer | 1-4=Trimestres, 5=Não, 6=N/A, 9=Ignorado | Gestação |
| `cs_raca` | integer | 1=Branca...5=Indígena, 9=Ignorado | Raça/cor declarada |
| `cs_escol_n` | integer | 0-4=Escolaridade, 5=N/A, 9=Ignorado | Escolaridade |

### Sintomas Clínicos

| Coluna | Tipo | Domínio | Descrição |
|---|---|---|---|
| `febre` | integer | 1=Sim, 2=Não, 9=Ignorado | Febre |
| `tosse` | integer | 1=Sim, 2=Não, 9=Ignorado | Tosse |
| `garganta` | integer | 1=Sim, 2=Não, 9=Ignorado | Dor de garganta |
| `dispneia` | integer | 1=Sim, 2=Não, 9=Ignorado | Dispneia |
| `desc_resp` | integer | 1=Sim, 2=Não, 9=Ignorado | Desconforto respiratório |
| `saturacao` | integer | 1=Sim, 2=Não, 9=Ignorado | Saturação O2 < 95% |
| `diarreia` | integer | 1=Sim, 2=Não, 9=Ignorado | Diarreia |
| `vomito` | integer | 1=Sim, 2=Não, 9=Ignorado | Vômito |
| `dor_abd` | integer | 1=Sim, 2=Não, 9=Ignorado | Dor abdominal |
| `fadiga` | integer | 1=Sim, 2=Não, 9=Ignorado | Fadiga |
| `perd_olft` | integer | 1=Sim, 2=Não, 9=Ignorado | Perda de olfato |
| `perd_pala` | integer | 1=Sim, 2=Não, 9=Ignorado | Perda de paladar |

### Fatores de Risco

| Coluna | Tipo | Descrição |
|---|---|---|
| `fator_risc` | integer | Possui fator de risco |
| `cardiopati` | integer | Doença cardiovascular crônica |
| `asma` | integer | Asma |
| `diabetes` | integer | Diabetes mellitus |
| `neurologic` | integer | Doença neurológica crônica |
| `pneumopati` | integer | Pneumopatia crônica |
| `imunodepre` | integer | Imunodeficiência/imunodepressão |
| `renal` | integer | Doença renal crônica |
| `obesidade` | integer | Obesidade |
| `tabag` | integer | Tabagismo |

Domínio de todos: 1=Sim, 2=Não, 9=Ignorado.

### Vacinação

| Coluna | Tipo | Descrição |
|---|---|---|
| `vacina_gripe` | integer | Vacinado contra gripe (1=Sim, 2=Não, 9=Ignorado) |
| `dt_ultima_dose_gripe` | date | Data da última dose contra gripe |
| `vacina_covid` | integer | Vacinado contra COVID-19 (1=Sim, 2=Não, 9=Ignorado) |
| `dose_1_cov` | date | Data da 1ª dose COVID |
| `dose_2_cov` | date | Data da 2ª dose COVID |
| `dose_ref` | date | Data do reforço |
| `dose_2ref` | date | Data do 2º reforço |
| `dose_adic` | date | Data da dose adicional |
| `dos_re_bi` | date | Data do reforço bivalente |
| `qtd_doses_covid_registradas` | integer | Count de doses com data preenchida |

### Internação, UTI e Severidade

| Coluna | Tipo | Descrição |
|---|---|---|
| `hospital` | integer | Internação hospitalar (1=Sim, 2=Não, 9=Ignorado) |
| `dt_interna` | date | Data de internação |
| `uti` | integer | Internação em UTI (1=Sim, 2=Não, 9=Ignorado) |
| `dt_entuti` | date | Data de entrada na UTI |
| `dt_saiduti` | date | Data de saída da UTI |
| `dias_uti` | integer | `datediff(dt_saiduti, dt_entuti)` |
| `suport_ven` | integer | 1=Invasivo, 2=Não invasivo, 3=Não, 9=Ignorado |

### Diagnóstico e Classificação

| Coluna | Tipo | Domínio | Descrição |
|---|---|---|---|
| `pcr_resul` | integer | 1-5, 9 | Resultado RT-PCR |
| `classi_fin` | integer | 1=Influenza, 2=Outro vírus, 3=Outro agente, 4=Não especificado, 5=COVID-19 | Classificação final |
| `criterio` | integer | 1=Laboratorial, 2=Clínico-epidemiológico, 3=Clínico, 4=Clínico-imagem | Critério de encerramento |

### Evolução

| Coluna | Tipo | Domínio | Descrição |
|---|---|---|---|
| `evolucao` | integer | 1=Cura, 2=Óbito, 3=Óbito outras causas, 9=Ignorado | Desfecho clínico |
| `dt_evoluca` | date | — | Data da alta ou óbito |
| `dt_encerra` | date | — | Data do encerramento do caso |

### 17 Flags Epidemiológicas

São colunas derivadas que simplificam os cálculos nas agregações Gold.
Valores: 0 ou 1.

| Flag | Regra de derivação | Uso na Gold |
|---|---|---|
| `flag_caso_srag` | `1` (literal) | Contagem de casos |
| `flag_obito_srag` | `evolucao = 2` | Numerador da taxa de mortalidade |
| `flag_obito_outras_causas` | `evolucao = 3` | Mortalidade total |
| `flag_cura` | `evolucao = 1` | Análise de desfecho |
| `flag_evolucao_informada` | `evolucao IN (1,2,3)` | **Denominador** da taxa de mortalidade |
| `flag_hospitalizado` | `hospital=1 OR dt_interna IS NOT NULL` | Denominador da taxa de UTI |
| `flag_uti` | `uti = 1` | Numerador da taxa de UTI |
| `flag_uso_suporte_ventilatorio` | `suport_ven IN (1,2)` | Severidade |
| `flag_vacinado_gripe` | `vacina_gripe = 1` | Numerador vacinação gripe |
| `flag_info_vacina_gripe` | `vacina_gripe IN (1,2)` | Denominador vacinação gripe |
| `flag_vacinado_covid` | `vacina_covid = 1` | Numerador vacinação COVID |
| `flag_info_vacina_covid` | `vacina_covid IN (1,2)` | Denominador vacinação COVID |
| `flag_covid_classificacao_final` | `classi_fin = 5` | Casos COVID |
| `flag_influenza_classificacao_final` | `classi_fin = 1` | Casos Influenza |
| `flag_outro_virus_classificacao_final` | `classi_fin = 2` | Outros vírus |
| `flag_srag_nao_especificado` | `classi_fin = 4` | Não especificados |

### Colunas Técnicas (SCD2 + Rastreabilidade)

| Coluna | Tipo | Descrição |
|---|---|---|
| `hash_registros` | string | SHA-256 do conteúdo — detecta mudanças para SCD2 |
| `dh_ultima_atualizacao` | timestamp | `COALESCE(dt_digita, dt_encerra, dt_notific, dh_ingestao_bronze)` — watermark |
| `dh_ingestao_silver` | timestamp | Timestamp do processamento |
| `registro_mais_atual` | boolean | true = versão vigente |
| `dh_inicio_vigencia` | timestamp | Quando esta versão ficou ativa |
| `dh_fim_vigencia` | timestamp | Quando foi substituída (null = ainda vigente) |
| `run_id` | string | UUID da execução |
| `nome_arquivo_origem` | string | Nome do CSV de origem |

---

## 2. `silver_cnes_leitos_uti`

Grão: 1 linha por estabelecimento de saúde (CNES) por competência (mês/ano).

| Coluna | Tipo | Descrição |
|---|---|---|
| `comp` | integer | Competência YYYYMM |
| `ano_mes` | string | Competência formatada YYYY-MM |
| `ano` | integer | Ano |
| `mes` | integer | Mês (1–12) |
| `regiao` | string | Região do Brasil |
| `uf` | string | Estado |
| `co_ibge` | string | Código IBGE (apenas dados 2026) |
| `municipio` | string | Nome do município (Title Case) |
| `cnes` | string | Código CNES do estabelecimento |
| `nome_estabelecimento` | string | Nome fantasia (Title Case) |
| `uti_total_exist` | integer | **Total de leitos UTI existentes** |
| `uti_total_sus` | integer | Leitos UTI SUS |
| `uti_adulto_exist` | integer | UTI Adulto (I, II e III) |
| `uti_pediatrico_exist` | integer | UTI Pediátrico |
| `uti_neonatal_exist` | integer | UTI Neonatal |
| `uti_queimado_exist` | integer | UTI Queimado |
| `uti_coronariana_exist` | integer | UTI Coronariana |
| `leitos_existentes` | integer | Total de leitos (não apenas UTI) |
| `run_id` | string | UUID da execução |
| `dh_ingestao_silver` | timestamp | Timestamp do processamento |

---

## 3. `silver_cnes_leitos_uti_por_uf`

Grão: 1 linha por UF + ano_mes — **denominador real da taxa de ocupação de UTI**.

| Coluna | Tipo | Descrição |
|---|---|---|
| `uf` | string | Estado |
| `regiao` | string | Região |
| `ano` | integer | Ano |
| `mes` | integer | Mês |
| `ano_mes` | string | YYYY-MM |
| `uti_total_exist` | integer | **Capacidade total de UTI por UF/mês** |
| `uti_adulto_exist` | integer | Capacidade UTI Adulto |
| `uti_pediatrico_exist` | integer | Capacidade UTI Pediátrico |
| `uti_neonatal_exist` | integer | Capacidade UTI Neonatal |
| `qtd_estabelecimentos` | integer | Número de CNES distintos |
| `run_id` | string | UUID |
| `dh_ingestao_silver` | timestamp | Timestamp |

**Como usar para calcular taxa de ocupação:**

```sql
SELECT
    s.sg_uf_residencia AS uf,
    date_format(s.dt_sin_pri, 'yyyy-MM') AS ano_mes,
    SUM(s.flag_uti) AS casos_uti_srag,
    l.uti_total_exist AS capacidade_uti,
    ROUND(SUM(s.flag_uti) / l.uti_total_exist, 4) AS taxa_ocupacao_uti
FROM silver_poc_srag_datasus s
JOIN silver_cnes_leitos_uti_por_uf l
    ON s.sg_uf_residencia = l.uf
    AND date_format(s.dt_sin_pri, 'yyyy-MM') = l.ano_mes
WHERE s.registro_mais_atual = true
GROUP BY s.sg_uf_residencia, date_format(s.dt_sin_pri, 'yyyy-MM'), l.uti_total_exist
```
