# Documentação dos Dashboards

**Projeto:** PoC SRAG — HealthCare Indicium  
**Autor:** Gabriel Ronney da Silva  
**Atualizado:** 2026-05-27

---

## Visão Geral

A PoC conta com dois dashboards complementares, ambos gerados com `matplotlib`
e `seaborn` diretamente no Databricks, sem ferramentas externas de BI.

| Dashboard | Notebook | Acesso |
|---|---|---|
| Monitoramento de SRAG | `08_dashboard_visualizacoes_srag.py` | [Abrir dashboard](https://dbc-a3683c7d-7729.cloud.databricks.com/editor/notebooks/2537046554484823/dashboards/6401b2e1-4a0d-47ff-832f-aefd2f9d77ef?o=2192137876212676) |
| Qualidade de Dados DMBOK | `09_dashboard_qualidade_dmbok.py` | [Abrir dashboard](https://dbc-a3683c7d-7729.cloud.databricks.com/editor/notebooks/2537046554484822/dashboards/7719a985-d5e2-4bd6-a8f1-52c728d03cc2?o=2192137876212676) |

> ⚠️ Os links requerem login no workspace Databricks.
> Credenciais para a banca: `poc.indicium.srag@gmail.com` — por canal privado.


---

## Dashboard 1 — Monitoramento de SRAG

**Notebook:** `dashboard/08_dashboard_visualizacoes_srag.py`  
**Propósito:** Visão epidemiológica operacional — tendências, volumes e taxas dos casos SRAG.

**Link direto:**
```
https://dbc-a3683c7d-7729.cloud.databricks.com/editor/notebooks/2537046554484823/dashboards/6401b2e1-4a0d-47ff-832f-aefd2f9d77ef?o=2192137876212676
```

### Fontes de dados

| Tabela Gold | Uso |
|---|---|
| `gold_agg_poc_srag_indicadores_relatorio` | KPIs consolidados — Gráfico 1 |
| `gold_agg_poc_srag_metricas_diarias` | Série temporal 30 dias — Gráficos 2, 4 |
| `gold_agg_poc_srag_metricas_mensais` | Série temporal 12 meses — Gráficos 3, 5 |
| `gold_fact_poc_srag_datasus` | Heatmap por UF — Gráfico 6 |
| `gold_dim_localidade_poc_srag_datasus` | Lookup UF/município — Gráfico 6 |

### Gráficos gerados

#### Gráfico 1 — Painel de KPIs (4 métricas obrigatórias)

Painel de 4 cartões com as métricas principais do período de 30 dias:

| KPI | Fonte | Interpretação |
|---|---|---|
| Taxa de Aumento de Casos | variação 7d vs 7d anteriores | Negativo = redução; positivo = crescimento |
| Taxa de Mortalidade | óbitos / casos com evolução informada | Calculada apenas sobre casos com desfecho |
| Taxa de Uso de UTI | casos SRAG em UTI / capacidade CNES/UF/mês | Não é ocupação real de leitos |
| Vacinação Registrada | vacinados / casos com info de vacina | Não é cobertura vacinal populacional |

Cada cartão exibe o valor principal, subtítulo com contexto e uma barra de cor
indicando o nível de atenção (verde = estável, laranja = atenção, vermelho = alerta).

---

#### Gráfico 2 — Casos Diários + Média Móvel 7 dias (últimos 30 dias)

- **Tipo:** Barras (casos diários) + linha (MM7d)
- **Fonte:** `gold_agg_poc_srag_metricas_diarias`
- **O que mostra:** Volume diário de novos casos registrados e a média móvel de
  7 dias para suavizar a sazonalidade semanal do SIVEP-Gripe.
- **Limitação declarada:** Dados dos últimos 7-14 dias podem estar subnotificados
  por atraso de digitação.

---

#### Gráfico 3 — Casos Mensais + Óbitos (últimos 12 meses)

- **Tipo:** Barras empilhadas (casos e óbitos) com eixo duplo
- **Fonte:** `gold_agg_poc_srag_metricas_mensais`
- **O que mostra:** Tendência de médio prazo — sazonalidade anual, ondas e picos.
  Permite identificar meses de maior pressão epidemiológica.

---

#### Gráfico 4 — Taxa de Mortalidade Diária

- **Tipo:** Linha com área sombreada
- **Fonte:** `gold_agg_poc_srag_metricas_diarias`
- **O que mostra:** Variação diária da taxa de mortalidade.
- **Atenção:** Taxa diária é mais volátil que a mensal — pequenas variações podem
  ser ruído estatístico, especialmente em dias com poucos casos registrados.

---

#### Gráfico 5 — Distribuição por Agente Etiológico (últimos 12 meses)

- **Tipo:** Área empilhada ou barras agrupadas
- **Fonte:** `gold_agg_poc_srag_metricas_mensais`
- **O que mostra:** Proporção mensal de casos por classificação final:
  COVID-19 (classi_fin=5), Influenza (1), Outros vírus (2), Outros agentes (3),
  Não especificado (4).
- **Uso:** Identifica dominância de agente etiológico por período.

---

#### Gráfico 6 — Heatmap de Casos por UF e Mês

- **Tipo:** Heatmap (UF × mês)
- **Fonte:** `gold_fact_poc_srag_datasus` + `gold_dim_localidade_poc_srag_datasus`
- **O que mostra:** Intensidade de casos SRAG por estado e por mês — permite
  identificar estados com maior carga epidemiológica e padrões geográficos.
- **Eixos:** UF no eixo Y, mês no eixo X, intensidade de cor proporcional ao volume.

---

## Dashboard 2 — Qualidade de Dados DMBOK

**Notebook:** `dashboard/09_dashboard_qualidade_dmbok.py`  
**Propósito:** Monitoramento da qualidade dos dados nas camadas Bronze e Gold,
baseado nas 5 dimensões DMBOK avaliadas pelo notebook `02_bronze_quality_srag.py`.

**Link direto:**
```
https://dbc-a3683c7d-7729.cloud.databricks.com/editor/notebooks/2537046554484822/dashboards/7719a985-d5e2-4bd6-a8f1-52c728d03cc2?o=2192137876212676
```

### Fontes de dados

| Tabela | Uso |
|---|---|
| `bronze_quality_poc_srag_datasus` | Dimensões Completude, Validade, Consistência, Unicidade, Atualidade |
| `gold_quality_poc_srag_datasus` | Dimensão Acurácia + qualidade consolidada |

**Importante — agrupamento por `run_id`:**
Cada execução do `02_bronze_quality_srag.py` gera um `run_id` único. Como cada
dimensão DMBOK é avaliada em uma célula separada, os timestamps das 40 regras
ficam diferentes dentro de uma mesma execução. O dashboard agrupa por `run_id`
(não por timestamp) para garantir que todas as dimensões sejam tratadas como
uma execução coesa. A execução selecionada é sempre a mais recente com todas
as 5 dimensões presentes.

### Gráficos gerados

#### Gráfico 1 — Semáforo de Qualidade Geral

- **Tipo:** Cartões com barras de severidade + círculo central com nível geral
- **Fonte:** Bronze Quality + Gold Quality
- **O que mostra:** Resumo executivo da última execução de qualidade.
  Para Bronze e Gold separadamente, exibe:
  - Distribuição de regras por severidade (CRÍTICO / ALTO / MÉDIO / BAIXO / ACEITÁVEL)
  - Nível geral da camada (o pior nível presente)
  - Data e hora da execução

**Thresholds:**

| Severidade | Percentual de validade | Ação |
|---|---|---|
| CRÍTICO | < 80% | Métrica comprometida |
| ALTO | 80% a 89% | Declarar limitação obrigatória |
| MÉDIO | 90% a 94% | Monitorar |
| BAIXO | 95% a 98% | Impacto pequeno |
| ACEITÁVEL | ≥ 99% | Nenhuma ação |

---

#### Gráfico 2 — Completude por Coluna (Bronze)

- **Tipo:** Barras horizontais com linhas de threshold
- **Fonte:** `bronze_quality_poc_srag_datasus` — dimensão Completude
- **O que mostra:** % de registros não-nulos para cada um dos 11 campos
  obrigatórios e essenciais avaliados.
- **Linhas de referência:** ACEITÁVEL (99%), ALTO (90%), CRÍTICO (80%)
- **Impacto:** Campos como `classi_fin`, `evolucao` e `uti` têm impacto direto
  nas taxas epidemiológicas do agente.

---

#### Gráfico 3 — Validade de Domínio por Coluna (Bronze)

- **Tipo:** Barras horizontais empilhadas (válidos + inválidos)
- **Fonte:** `bronze_quality_poc_srag_datasus` — dimensão Validade
- **O que mostra:** Para cada uma das 18 colunas com domínio definido pelo
  SIVEP-Gripe, a proporção de registros com valor dentro do domínio esperado.
- **Cores:** Verde claro = válidos; laranja/vermelho = inválidos (cor varia com severidade)
- **Exemplo:** `cs_sexo` aceita M, F, I — qualquer outro valor é inválido.

---

#### Gráfico 4 — Consistência Temporal entre Datas (Bronze)

- **Tipo:** Barras horizontais empilhadas
- **Fonte:** `bronze_quality_poc_srag_datasus` — dimensão Consistência
- **O que mostra:** Para cada uma das 6 relações temporais avaliadas,
  a proporção de registros cronologicamente coerentes.
- **Relações avaliadas:**
  - `dt_sin_pri` ≤ `dt_notific`
  - `dt_interna` ≥ `dt_sin_pri`
  - `dt_entuti` ≥ `dt_sin_pri`
  - `dt_saiduti` ≥ `dt_entuti`
  - `dt_evoluca` ≥ `dt_sin_pri`
  - `dt_encerra` ≥ `dt_notific`

---

#### Gráfico 5 — Radar DMBOK (Bronze vs Gold)

- **Tipo:** Gráfico radar / aranha com 6 eixos
- **Fonte:** Bronze Quality + Gold Quality
- **O que mostra:** Comparativo visual das 6 dimensões DMBOK entre as camadas
  Bronze (linha azul sólida com marcador circular) e Gold (linha âmbar tracejada
  com marcador quadrado).
- **Dimensões:** Completude, Validade, Consistência, Unicidade, Atualidade, Acurácia
- **Interpretação:** Dimensões não avaliadas em uma camada aparecem no limite
  inferior do eixo (0.70) com rótulo "não avaliado" — não significa problema,
  apenas que a dimensão não se aplica àquela camada.
- **Limite do eixo:** 70% a 103% — abaixo de 80% é área vermelha (crítico).

---

#### Gráfico 6 — Evolução Histórica da Qualidade por Execução

- **Tipo:** Gráficos de barras (alertas) + linha (score médio) — Bronze e Gold lado a lado
- **Fonte:** Histórico completo de `bronze_quality` e `gold_quality` (todas as execuções)
- **O que mostra:** Como a qualidade dos dados evoluiu ao longo das execuções
  do pipeline — demonstra a maturidade do processo.
- **Eixo esquerdo:** Número de alertas por severidade (CRÍTICO, ALTO, MÉDIO)
- **Eixo direito:** Score médio de validade (%) da execução
- **Agrupamento:** Por `run_id` — apenas execuções com ≥ 3 regras são exibidas
  (execuções parciais são filtradas)

---

#### Gráfico 7 — Acurácia e Atualidade das Métricas Gold

- **Tipo:** Tabela visual com colunas coloridas por severidade
- **Fonte:** `gold_quality_poc_srag_datasus` — dimensões Acurácia e Atualidade
- **O que mostra:** Validação de sanidade das métricas calculadas na Gold:
  - Regras de Acurácia: taxa de mortalidade entre 0 e 60%, taxa UTI entre 0 e 100%
  - Regras de Atualidade: defasagem entre data máxima e hoje ≤ 30 dias
- **Colunas:** Regra | Dimensão | Resultado (%) | Severidade | Impacto analítico

---

---

---

*PoC: Certificação AI Engineering — Indicium | Maio/2026*
