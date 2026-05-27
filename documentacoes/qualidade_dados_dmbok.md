# Qualidade de Dados — Dimensões DMBOK

**Projeto:** PoC SRAG — HealthCare Indicium  
**Autor:** Gabriel Ronney da Silva  
**Atualizado:** 2026-05-27

---

## Contexto

A qualidade dos dados é o que separa uma análise confiável de uma análise que
induz decisões erradas. Num contexto de saúde pública — onde os números podem
embasar políticas de vacinação, alocação de leitos e alertas epidemiológicos —
isso é especialmente crítico.

Esta PoC aplica 6 dimensões de qualidade do DMBOK (Data Management Body of Knowledge),
distribuídas entre as camadas Bronze e Gold. Os resultados são gravados em
`bronze_quality_poc_srag_datasus` e `gold_quality_poc_srag_datasus`, ambas em APPEND,
formando uma série histórica de execuções rastreada por `run_id`.

**Dashboard de qualidade:**
`https://dbc-a3683c7d-7729.cloud.databricks.com/editor/notebooks/2537046554484822/dashboards/7719a985-d5e2-4bd6-a8f1-52c728d03cc2?o=2192137876212676`

---

## Thresholds de Severidade

Aplicados em todas as dimensões mensuráveis:

| Percentual de validade | Severidade | Ação recomendada |
|---|---|---|
| < 80% | CRÍTICO | Métrica comprometida — declarar obrigatoriamente |
| 80% a 89% | ALTO | Métrica afetada — incluir ressalva no relatório |
| 90% a 94% | MÉDIO | Impacto moderado — monitorar |
| 95% a 98% | BAIXO | Impacto pequeno |
| ≥ 99% | ACEITÁVEL | Nenhuma ação necessária |

---

## Dimensão 1 — Completude

**O que mede:** Se os campos obrigatórios e essenciais estão preenchidos.

**Onde:** Notebook `02_bronze_quality_srag.py` — 11 regras.

| Campo | Impacto se nulo |
|---|---|
| `nu_notific` | Impede deduplicação e rastreabilidade |
| `dt_sin_pri` | Exclui o caso de todas as séries temporais Gold |
| `dt_notific` | Afeta watermark e ordenação temporal |
| `sg_uf` | Inviabiliza análise geográfica |
| `co_mun_res` | Inviabiliza granularidade municipal |
| `classi_fin` | Afeta todas as taxas por agente etiológico (COVID, Influenza...) |
| `evolucao` | Afeta diretamente a taxa de mortalidade |
| `uti` | Afeta taxa de uso de UTI |
| `vacina` | Afeta taxa de vacinação contra gripe |
| `vacina_cov` | Afeta taxa de vacinação COVID |
| `hospital` | Denominador da taxa de uso de UTI |

---

## Dimensão 2 — Validade

**O que mede:** Se os valores estão dentro dos domínios definidos pelo SIVEP-Gripe.

**Onde:** Notebook `02_bronze_quality_srag.py` — 18 regras.

| Coluna | Valores válidos |
|---|---|
| `cs_sexo` | M, F, I |
| `tp_idade` | 1=Dia, 2=Mês, 3=Ano |
| `cs_gestant` | 1 a 6, 9 |
| `cs_raca` | 1=Branca, 2=Preta, 3=Amarela, 4=Parda, 5=Indígena, 9=Ignorado |
| `febre`, `tosse`, `dispneia`, `saturacao` | 1=Sim, 2=Não, 9=Ignorado |
| `fator_risc`, `vacina`, `vacina_cov`, `uti`, `hospital` | 1=Sim, 2=Não, 9=Ignorado |
| `evolucao` | 1=Cura, 2=Óbito, 3=Óbito outras causas, 9=Ignorado |
| `classi_fin` | 1=Influenza, 2=Outro vírus, 3=Outro agente, 4=Não especificado, 5=COVID-19 |
| `suport_ven` | 1=Invasivo, 2=Não invasivo, 3=Não, 9=Ignorado |
| `pcr_resul` | 1 a 5, 9 |
| `criterio` | 1 a 4 |

Valores fora do domínio podem distorcer contagens por agente etiológico
e classificação final — impacto direto nos KPIs do agente.

---

## Dimensão 3 — Consistência

**O que mede:** Se as datas do caso são cronologicamente coerentes entre si.

**Onde:** Notebook `02_bronze_quality_srag.py` — 6 regras.

| Relação | Regra | Justificativa clínica |
|---|---|---|
| `dt_sin_pri` ≤ `dt_notific` | Sintomas antes da notificação | Não é possível notificar antes dos sintomas |
| `dt_interna` ≥ `dt_sin_pri` | Internação após sintomas | Presupõe doença prévia |
| `dt_entuti` ≥ `dt_sin_pri` | Entrada UTI após sintomas | Idem |
| `dt_saiduti` ≥ `dt_entuti` | Saída UTI após entrada | Cronologia obrigatória |
| `dt_evoluca` ≥ `dt_sin_pri` | Alta/óbito após sintomas | Desfecho pressupõe doença |
| `dt_encerra` ≥ `dt_notific` | Encerramento após notificação | Encerramento é posterior ao registro |

Datas invertidas geram ruído nas séries temporais e distorcem `dias_uti`.

---

## Dimensão 4 — Unicidade

**O que mede:** Se cada caso SRAG aparece apenas uma vez (sem duplicatas não intencionais).

**Onde:** Notebook `02_bronze_quality_srag.py` — 2 regras.

| Chave | O que detecta |
|---|---|
| `nu_notific` simples | Duplicatas diretas do mesmo número de notificação |
| `nu_notific` + `dt_sin_pri` + `dt_notific` | Reenvios do mesmo caso com mesmas datas |

**Como é tratado:** O SCD2 (MERGE) na Silver usa `hash_caso` como chave de negócio.
Casos com mesmo `nu_notific` e conteúdo diferente (atualização) têm a versão
anterior encerrada (`dh_fim_vigencia` preenchido, `registro_mais_atual = false`)
e a nova versão inserida com `registro_mais_atual = true`.

---

## Dimensão 5 — Atualidade

**O que mede:** Se os dados refletem o estado recente da realidade.

**Onde:** Notebook `02_bronze_quality_srag.py` — 3 regras.

| Indicador | O que mede |
|---|---|
| Data máxima de `dt_sin_pri` | Último caso com sintomas registrado |
| Data máxima de `dt_notific` | Última notificação registrada |
| Data máxima de `dt_digita` | Último registro digitado no SIVEP |

**Thresholds de defasagem:**

| Defasagem | Interpretação |
|---|---|
| ≤ 7 dias | Dataset atualizado — métricas confiáveis |
| 8 a 30 dias | Defasagem normal para o SIVEP-Gripe — declarar limitação |
| > 30 dias | Dataset desatualizado — aviso obrigatório no relatório |

O SIVEP-Gripe tem atraso estrutural de 7 a 30 dias por conta do tempo de
digitação nos serviços de saúde. Esse comportamento é esperado e sempre
declarado nas respostas do agente.

---

## Dimensão 6 — Acurácia

**O que mede:** Se as métricas calculadas na Gold fazem sentido epidemiológico.

**Onde:** Notebook `gold_quality_srag.py` — validação de sanidade das métricas.

| Métrica | Validação | Limite esperado |
|---|---|---|
| `taxa_mortalidade_30d` | Entre 0 e 0.60 | Literatura indica até ~50% para SRAG hospitalizado |
| `taxa_uso_uti_30d` | Entre 0 e 1.0 | UTI ≤ total de hospitalizados |
| `taxa_vacinacao_gripe_30d` | Entre 0 e 1.0 | Vacinados ≤ com informação |
| `taxa_vacinacao_covid_30d` | Entre 0 e 1.0 | Vacinados ≤ com informação |

**Importante:**

- `taxa_uso_uti` mede casos SRAG que usaram UTI — não é taxa de ocupação real de leitos.
- `taxa_vacinacao` mede vacinados entre casos SRAG — não é cobertura vacinal populacional.

---

## Resumo por camada

```
02_bronze_quality_srag.py
    40 regras | 5 dimensões | run_id único por execução
    → Completude  (11 regras)
    → Validade    (18 regras)
    → Consistência (6 regras)
    → Unicidade    (2 regras)
    → Atualidade   (3 regras)

gold_quality_srag.py
    → Acurácia (sanidade das métricas Gold)
    → Completude de SKs (dim_tempo, dim_classificacao)
    → Atualidade (defasagem do dataset)
    → Herda alertas da Bronze Quality
```

---

## Como o agente usa a qualidade

Antes de emitir qualquer métrica, o agente verifica `gold_quality_poc_srag_datasus`.
Se houver alertas CRÍTICO ou ALTO, o relatório inclui aviso explícito:

```
⚠️ Aviso de Qualidade (ALTO): A coluna 'evolucao' apresenta 87% de validade.
Impacto: A taxa de mortalidade pode estar subestimada — casos sem evolução
informada são excluídos do denominador.
```

Todos os guardrails acionados ficam registrados em `gold_audit_agent_poc_srag_datasus`.
