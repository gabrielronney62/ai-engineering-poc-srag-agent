# Governança LGPD — PoC SRAG DATASUS

**Projeto:** PoC SRAG — HealthCare Indicium  
**Autor:** Gabriel Ronney da Silva  
**Ambiente:** Databricks (controlado, não produtivo)  
**Criado em:** 2025-06-26 | **Atualizado:** 2026-05-27

---

## Por que isso importa

O SIVEP-Gripe registra dados clínicos de pacientes hospitalizados — dados que têm
valor enorme para a saúde pública, mas que também contêm informações pessoais sensíveis.

Esta PoC trabalha com dados reais de domínio público e demonstra que é possível
construir uma solução analítica epidemiológica completa **sem expor dados pessoais**
em nenhuma camada de consumo.

---

## 1. Classificação de Dados Pessoais

### Categoria 1 — PII Direto (15 campos)

Identificam a pessoa diretamente. LGPD Art. 5º, I.

| Campo CSV | Descrição | Tratamento |
|---|---|---|
| `NU_CPF` | CPF | Hash SHA-256 → Silver; valor real → cofre PII |
| `NU_CNS` | Cartão Nacional de Saúde | Idem |
| `NM_PACIENT` | Nome completo | Idem |
| `NM_MAE_PAC` | Nome da mãe | Idem |
| `NU_CEP` | CEP | Idem |
| `NM_BAIRRO` | Bairro | Idem |
| `NM_LOGRADO` | Logradouro | Idem |
| `NU_NUMERO` | Número do endereço | Idem |
| `NM_COMPLEM` | Complemento | Idem |
| `NU_DDD_TEL` | DDD | Idem |
| `NU_TELEFON` | Telefone | Idem |
| `OBSERVA` | Observações livres (pode conter PII implícito) | Idem |
| `NOME_PROF` | Nome do profissional notificante | Idem |
| `REG_PROF` | Registro profissional | Idem |
| `VG_PROF` | Profissional de vigilância genômica | Idem |

### Categoria 2 — Quasi-identificador de Alto Risco (1 campo)

| Campo | Risco | Tratamento |
|---|---|---|
| `DT_NASC` | Combinado com `sg_uf` + `cs_sexo`: ~87% de re-identificação (Sweeney, 2002) | Hash SHA-256 na Silver como `dt_nasc_hash`; valor real → cofre PII |

A Silver tem `faixa_etaria` e `idade_anos_calculada` como substitutos analíticos.
Manter `dt_nasc` seria exposição desnecessária — violação do princípio de minimização
de dados (LGPD Art. 6º, III).

**Referência:** Sweeney, L. (2002). k-anonymity: A Model for Protecting Privacy.
International Journal on Uncertainty, Fuzziness and Knowledge-based Systems, 10(5), 557–570.

### Categoria 3 — Quasi-identificadores de Baixo Risco (mantidos)

| Campo | Motivo para manter |
|---|---|
| `id_mn_resi` | Análise epidemiológica por município — baixo risco isolado |
| `nm_un_inte` | Hospital de internação é estabelecimento, não pessoa |
| `morb_desc` | Texto clínico sem identificador pessoal |

---

## 2. Algoritmo de Pseudoanonimização

```python
# SHA-256 determinístico com salt
hash = SHA-256(SALT_PII + "||" + valor_original)
# SALT_PII lido via Databricks Secrets — nunca em texto claro no código

# Determinístico: mesmo CPF sempre gera mesmo hash
# Permite JOINs entre Silver e cofre PII via hash
```

> **Atenção:** Salt fixo é simplificação de PoC. Em produção: Azure Key Vault,
> salt único por campo e rotação periódica.

---

## 3. Estrutura do Cofre PII

**Tabela:** `certificacao_indicium.auditoria_pii.mapa_identificacao_dados_srag_datasus`

Única tabela do Lakehouse onde hash e valor original coexistem.
Acesso restrito — o agente é bloqueado via guardrail G4.

```
hash_caso         → chave de ligação com Silver/Gold
nu_cpf_hash       → hash do CPF
nu_cpf            → CPF em texto claro
dt_nasc_hash      → hash da data de nascimento
dt_nasc           → data de nascimento em texto claro
[...demais campos PII...]
dh_carga_pii      → timestamp de gravação
```

**Por que OVERWRITE?**
Quando um titular solicita exclusão (LGPD Art. 18), basta rodar o pipeline
com o dado removido na fonte. O mapa é reconstruído sem o registro excluído.

---

## 4. Fluxo de Pseudoanonimização

```
Bronze (contém CPF, nome, dt_nasc em texto claro)
    ↓
Notebook 03 — capturar PII ANTES de qualquer seleção de colunas
    ↓
Gerar hashes SHA-256 → gravar mapa em auditoria_pii (OVERWRITE)
    ↓
Silver produtiva:
    → ZERO PII direto em texto claro
    → dt_nasc_hash visível (hash, sem valor real)
    → faixa_etaria + idade_anos_calculada como substitutos
    ↓
Gold / Agente — apenas agregações (contagens, taxas, médias)
    → nunca acessa auditoria_pii
    → nunca expõe hash_caso individualmente
```

---

## 5. Guardrails do Agente

| Guardrail | Momento | O que faz |
|---|---|---|
| G1 — Identificação individual | Entrada | Bloqueia "nome do paciente", "prontuário", "dados pessoais" |
| G2 — CPF/CNS | Entrada e Saída | Regex detecta padrão de CPF e bloqueia |
| G3 — Audit log | Saída | Toda consulta gravada em `gold_audit_agent_poc_srag_datasus` |
| G4 — Cofre PII | Tool | Agente sem acesso ao schema `auditoria_pii` |
| G5 — Alerta qualidade | Saída | Aviso obrigatório se DMBOK CRÍTICO ou ALTO |
| G6 — Estrutura obrigatória | Saída | [DADOS OBSERVADOS] / [CONTEXTO EXTERNO] / [ANÁLISE] |
| G7 — Correção de linguagem | Saída | "cobertura vacinal" → "vacinação registrada entre casos SRAG" |
| G8 — Métricas apenas do banco | Sistema | LLM não inventa métricas — usa apenas dados das tools |

---

## 6. Limitações desta PoC

| Limitação | Solução em Produção |
|---|---|
| Salt PII fixo no código | Azure Key Vault + rotação periódica |
| Cofre PII no mesmo catálogo | Catálogo dedicado `pii_vault` |
| Guardrails em camada de aplicação | Column masking Unity Catalog + Row-level security |
| Sem política formal de retenção | Política LGPD com DPO registrado no ANPD |

*Para a decisão arquitetural de manter o cofre PII no mesmo catálogo,*
*ver `documentacoes/decisao_arquitetural_pii_poc.md`.*
