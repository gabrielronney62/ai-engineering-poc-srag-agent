# Decisão Arquitetural — Cofre PII na PoC

**ADR (Architecture Decision Record) nº 001**  
**Projeto:** PoC SRAG — HealthCare Indicium  
**Autor:** Gabriel Ronney da Silva  
**Data:** 2025-06-26 | **Status:** Aceita (escopo PoC)

---

## Contexto

As boas práticas recomendam que dados sensíveis (PII) sejam armazenados em
catálogo dedicado, isolado dos dados analíticos, com ACLs restritas e auditoria formal.

O dataset SIVEP-Gripe contém CNS, endereço e outras informações pessoais.
O pipeline aplica pseudoanonimização na Silver e persiste o mapa de identificação
em tabela separada.

---

## Decisão

**Para esta PoC, o cofre PII será mantido dentro do catálogo `certificacao_indicium`,
no schema `auditoria_pii`.**

Tabela: `certificacao_indicium.auditoria_pii.mapa_identificacao_dados_srag_datasus`

---

## Justificativa

| Critério | Justificativa |
|---|---|
| Ambiente controlado | PoC em ambiente não produtivo, dados de domínio público |
| Simplificação de setup | Criar catálogo dedicado requer permissões de admin fora do escopo |
| Separação lógica garantida | Schema `auditoria_pii` isola logicamente os dados sensíveis |
| Rastreabilidade mantida | Mapa PII é gerado, versionado e auditável |
| Documentação explícita | Decisão comunicada e marcada como simplificação de PoC |

---

## Alternativas Consideradas

### Alternativa 1: Catálogo dedicado `pii_vault` (Recomendada para Produção)

```
pii_vault.auditoria_pii.mapa_identificacao_dados_srag_datasus
```

**Vantagens:** Isolamento físico e lógico total, ACLs por catálogo, auditoria
Unity Catalog por catálogo, column masking por política de catálogo.

**Por que não foi feita:** Requer `CREATE CATALOG` no Unity Catalog — fora do escopo desta PoC.

### Alternativa 2: Não criar o mapa PII (Descartada)

Perda de rastreabilidade e impossibilidade de reverter pseudoanonimização
em caso de necessidade legal legítima.

### Alternativa 3: Manter PII na Silver sem pseudoanonimização (Descartada)

Viola o princípio de minimização de dados da LGPD (Art. 6º, III) e expõe
PII desnecessariamente nas camadas analíticas.

---

## Consequências

**Positivas:**
- Pipeline funciona no catálogo existente sem dependências adicionais.
- Separação lógica via schema é suficiente para o escopo da PoC.
- Facilita demonstração e avaliação pela banca.

**Negativas / Riscos Aceitos:**
- Sem isolamento físico de catálogo.
- ACLs de catálogo não aplicáveis (apenas schema/tabela).
- Requer disciplina de acesso (não automatizada por infraestrutura).

---

## Plano de Evolução (Produção)

1. Criar catálogo dedicado `pii_vault` com permissões de Metastore Admin
2. Aplicar Column Masking via Unity Catalog Data Masking Policies
3. Configurar Row-level Security para o schema PII
4. Substituir salt fixo por Databricks Secrets (Azure Key Vault)
5. Implementar política de retenção e exclusão compatível com LGPD
6. Registrar o Encarregado de Dados (DPO) no ANPD

---

> **Aviso:** Para fins de PoC, o cofre PII foi mantido no catálogo `certificacao_indicium`.
> Em ambiente produtivo, recomenda-se isolar dados sensíveis em catálogo dedicado,
> com ACLs restritas, auditoria formal e políticas de acesso compatíveis com LGPD.

---

## ADR-002 — Quasi-identificadores: dt_nasc pseudoanonimizado, geográficos mantidos

**Data:** 2026-05-26 | **Status:** Aceita

### Contexto

Análise da Silver revelou que `dt_nasc` (data de nascimento) estava em texto
claro. Combinada com `sg_uf` e `cs_sexo`, forma um quasi-identificador de alto risco.

### Decisão

`dt_nasc` foi pseudoanonimizado: valor original vai para o cofre PII (hash + texto claro),
e a Silver recebe `dt_nasc_hash`. Substitutos analíticos disponíveis: `faixa_etaria`
e `idade_anos_calculada`.

Quasi-identificadores de baixo risco (`id_mn_resi`, `nm_un_inte`) foram mantidos
na Silver por necessidade epidemiológica.

### Justificativa técnica

Sweeney (2002) demonstrou que data de nascimento + CEP + sexo re-identifica ~87%
dos americanos. Para este dataset: `dt_nasc + sg_uf + cs_sexo` tem poder de
re-identificação similar no contexto brasileiro.

### Consequências

- Silver tem `dt_nasc_hash` no lugar de `dt_nasc`
- `faixa_etaria` e `idade_anos_calculada` são suficientes para todas as análises
- Mapa PII contém 16 campos × 2 colunas (hash + texto claro)
