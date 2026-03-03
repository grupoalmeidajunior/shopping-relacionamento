# Contexto do Projeto: Shopping Relacionamento

## Visao Geral

Dashboard **leve e focado** em acoes de relacionamento para os shoppings Almeida Junior. Cada shopping tem login proprio e ve apenas seus consumidores, com filtros por perfil, segmento e loja. Versao simplificada do Dashboard Perfil de Cliente, com foco em uso pratico pelos times de marketing dos shoppings.

**URL do Dashboard:** https://shopping-relacionamento.streamlit.app
**Repositorio GitHub:** https://github.com/carlosgravi/shopping-relacionamento

---

## Estrutura de Diretorios

```
C:\util\Docker_Airflow\Shopping_Relacionamento\
├── .streamlit/
│   ├── config.toml              # Tema (identidade visual AJ)
│   ├── secrets.toml             # Credenciais (nao versionado)
│   └── secrets.toml.exemplo     # Template de credenciais
├── .github/
│   └── workflows/
│       └── sincronizar_dados.yml  # Sync automatico com dashboard-perfil-cliente
├── Resultados/
│   ├── Completo/
│   │   ├── top_consumidores_rfv.csv
│   │   └── RFV/
│   │       ├── cliente_loja.csv
│   │       └── loja_info.csv
│   ├── Por_Ano/2025, 2026/       # Mesma estrutura
│   ├── Por_Trimestre/2025_Q1..2026_Q1/
│   └── Por_Mes/2025_01..2026_02/
├── AJ.jpg                        # Logo Almeida Junior
├── app.py                        # App principal (~1716 linhas)
├── cliente_categoria.csv         # Categorias AJFANS (8.3MB)
├── ranking_ajfans.csv            # Ranking AJFANS completo (88MB)
├── packages.txt                  # Dependencias de sistema (libffi-dev)
├── requirements.txt              # Dependencias Python (7)
├── CONTEXTO_PROJETO.md           # Este documento
└── .gitignore
```

---

## Dados

### top_consumidores_rfv.csv
- **Separador:** `;` | **Decimal:** `,` | **Encoding:** utf-8-sig
- **Todos os clientes** de cada shopping (sem limite de top_n)
- **Colunas principais:** Cliente_ID, Nome, CPF, Email, Celular, Endereco, Genero, Shopping, Valor_Total, Frequencia_Compras, Recencia_Dias, Segmento_Principal, Loja_Favorita_Shopping, Perfil_Cliente, Score_Total_RFV (3-15)

### cliente_loja.csv
- **Separador:** `,` | **Encoding:** utf-8-sig
- **Colunas:** cliente_id, loja_nome, n (visitas), valor

### loja_info.csv
- **Separador:** `,` | **Encoding:** utf-8-sig
- **Colunas:** loja_nome, segmento, shopping, valor, cupons

### cliente_categoria.csv (AJFANS)
- **Encoding:** latin-1
- **Colunas:** cliente_id, shopping_id, categoria (MegaFan/SuperFan/NewFan)

### ranking_ajfans.csv
- **Separador:** `;` | **Encoding:** utf-8-sig
- **Colunas:** cliente_id, categoria, shopping_nome, shopping_sigla, valor_total, qtd_cupons, ticket_medio, nome, email, celular, cidade, estado

---

## Periodos Disponiveis

| Tipo | Periodos |
|------|----------|
| Completo | Todos os dados historicos |
| Por Ano | 2025, 2026 |
| Por Trimestre | 2025_Q1, 2025_Q2, 2025_Q3, 2025_Q4, 2026_Q1 |
| Por Mes | 2025_01 a 2025_12, 2026_01, 2026_02 |

Multi-selecao de periodos: ao combinar, dados sao agregados (soma valor/frequencia, min recencia, moda categorias).

---

## Autenticacao

- Formulario customizado com `st.form()` + `bcrypt.checkpw()`
- 7 usuarios fixos no `secrets.toml`: admin + 6 shoppings
- Senha padrao: XX@Dash2026 (XX = sigla do shopping)
- Admin: ve todos os shoppings + pagina Administracao
- Rate limiting: 5 tentativas, bloqueio 15min (Google Sheets)
- Alertas por email para squadaj@almeidajunior.com.br

### Usuarios

| Username | Shopping | Role |
|----------|----------|------|
| admin | Balneario Shopping | admin |
| cs | Continente Shopping | viewer |
| bs | Balneario Shopping | viewer |
| nk | Neumarkt Shopping | viewer |
| nr | Norte Shopping | viewer |
| gs | Garten Shopping | viewer |
| ns | Nacoes Shopping | viewer |

---

## Paginas do Dashboard

### 1. Dashboard (pagina_dashboard)
- **Header** com intro explicativa para nao-expertes
- **Filtros:** Perfil (VIP/Premium/Potencial/Pontual), Segmento, Loja (cascading)
- **KPIs:** Total Clientes, Valor Total, % VIP, Ticket Medio (com help tooltips)
- **Tabela:** Ranking, Cliente_ID, Primeiro_Nome, Nome_Completo, Email, Celular, Bairro, Cidade, Genero, Valor_Total, Valor_Total_Filtrado, Perfil_Cliente
- **Download:** CSV (`;` separator) e Excel
- **Graficos (3 tabs):**
  - Por Perfil: pizza concentracao de valor + barras valor medio
  - Por Segmento: barras horizontal top 10 (cor accent do shopping)
  - Por Loja: barras horizontal top 10 (cor accent2 do shopping)
- **Acoes Recomendadas:**
  - VIPs em Risco (recencia > 60 dias)
  - Proximos de Upgrade (gap <= 2 pontos RFV): Pontual→Potencial, Potencial→Premium, Premium→VIP
  - Segmento Dominante
  - Loja Destaque
  - Clientes Inativos (recencia > 90 dias)

### 2. AJFANS (pagina_ajfans)
- **KPIs:** Total Cadastros, MegaFan, SuperFan, NewFan
- **3 tabs:**
  - Distribuicao: pizza + barras por categoria
  - Ranking: top N consumidores com filtro de categoria
  - Lista Completa: todos os clientes com download

### 3. Administracao (somente admin)
- **5 tabs:** Logs de Login, Filtros, Downloads, Seguranca, Rate Limit
- Dados lidos do Google Sheets via `ler_aba_como_df()` (tolerante a cabecalhos duplicados)

---

## Identidade Visual (Manual House Materiais)

### Cores Base
- **Azul Navy:** #031835 (sidebar, textos principais)
- **Cinza Gelo:** #dfe2e6 (backgrounds secundarios)

### Paleta por Shopping
| Shopping | Accent | Accent2 | Light | Chart Sequence |
|----------|--------|---------|-------|----------------|
| Neumarkt | #226275 | #0f3643 | #d5b8b6 | #226275, #0f3643, #d5b8b6, #5f605c |
| Balneario | #8a6cae | #152d52 | #cda7b9 | #8a6cae, #152d52, #cda7b9, #0f3457 |
| Continente | #f1716e | #424872 | #aea0ae | #f1716e, #424872, #716480, #aea0ae |
| Garten | #6563ab | #7790c9 | #4d1d58 | #6563ab, #7790c9, #4d1d58, #3f4269 |
| Norte | #185665 | #9c688d | #0f7171 | #185665, #9c688d, #0f7171, #2a1446 |
| Nacoes | #014b6f | #8e3e83 | #8a6cae | #014b6f, #8e3e83, #2e184c, #8a6cae |

### Cores de Perfil
- VIP: #C9A84C (ouro antigo)
- Premium: #8A8D93 (prata sofisticado)
- Potencial: #B07D4B (bronze)
- Pontual: #8E9AAF (slate)

### Tipografia
- Headers: weight 400, letter-spacing 0.06em (brand: clean, espaçado)
- Labels: uppercase, letter-spacing 0.05em
- Sidebar titulo: weight 300, letter-spacing 0.12em

---

## Sincronizacao de Dados

### GitHub Actions Workflow
- **Arquivo:** `.github/workflows/sincronizar_dados.yml`
- **Execucao:** Segunda-feira 6h BRT (9h UTC) + manual + repository_dispatch
- **Fonte:** repo `carlosgravi/dashboard-perfil-cliente`
- **Destino:** pasta `Resultados/`
- **Periodos sincronizados:** Completo + 2025/2026 (ano, trimestre, mes)
- **CSVs AJFANS:** cliente_categoria.csv + ranking_ajfans.csv (da raiz do repo fonte)
- **Requer:** secret `REPO_ACCESS_TOKEN` configurado no repo

### Geracao de Dados
Os dados sao gerados pelo script `gerar_analise_completa.py` no repo dashboard-perfil-cliente:
- `gerar_dados_periodo()` — gera todos os CSVs (periodos full)
- `gerar_dados_periodo_leve()` — gera apenas 3 CSVs (periodos mensais)
- `top_n=None` — sem limite de clientes por shopping

---

## Logging (Google Sheets)

| Aba | Colunas |
|-----|---------|
| logins | timestamp, usuario, nome, shopping, ip |
| filtros | timestamp, usuario, shopping, filtro, valor |
| downloads | timestamp, usuario, shopping, arquivo, registros |
| seguranca | timestamp, tipo, username, client_id, detalhes, ip |
| rate_limit | client_id, tentativas, ultima_tentativa, bloqueado_ate |

**Spreadsheet ID:** 1A95pck9X18NvScQmYV3MHnk3nUGOsiphG6fnezn2z5I
**Service Account:** streamlit-dashboard@dashboard-almeida-junior.iam.gserviceaccount.com

---

## Requirements

```
streamlit>=1.28.0
pandas>=2.0.0
plotly>=5.24.0
openpyxl>=3.1.0
bcrypt>=4.0.0
gspread>=5.12.0
google-auth>=2.23.0
```

Apenas 7 dependencias (vs 17+ do dashboard principal).

---

## Commits Recentes

| Commit | Descricao |
|--------|-----------|
| a71a0bd | feat: remodelar dashboard com identidade visual Almeida Junior |
| 21489cf | fix: corrigir cabecalhos das abas do Google Sheets |
| 5e7fecc | fix: corrigir erro de cabecalhos duplicados no Google Sheets |
| cd851c6 | feat: adicionar explicacoes detalhadas para usuarios nao-expertes |
| 2a1a290 | data: adicionar ranking_ajfans.csv (dados AJFANS) |
| b3e2904 | feat: pagina AJFANS com categorias de fidelidade |
| 376c052 | fix: upgrade usa Score_Total_RFV em vez de Valor_Total |
| 474d67c | fix: restaurar cards Segmento Dominante, Loja Destaque e Inativos |
| 008b39f | feat: analise de upgrade por perfil com download de listas |
| 14b2735 | feat: colunas da tabela - ID Cliente, Primeiro Nome, Valor Filtrado |
| 6d4a747 | fix: grafico de lojas filtra por segmento e loja selecionados |
| 8cae5ea | fix: KPIs e graficos respondem aos filtros selecionados |
| c70da2f | fix: grafico pizza mostra concentracao de valor em vez de contagem |
| f866499 | feat: multi-selecao de periodos com agregacao + suporte a meses |
| 7bb383c | feat: filtro de lojas encadeado por segmento selecionado |
| 87233c8 | feat: dashboard Shopping Relacionamento completo |

---

## Historico de Sessoes

### Sessao 1 (26/02/2026) — Criacao do Dashboard
- Criado dashboard completo do zero (app.py ~900 linhas)
- Autenticacao bcrypt + rate limiting + Google Sheets logging
- Filtros (perfil, segmento, loja), KPIs, tabela, graficos (3 tabs)
- Acoes Recomendadas (VIPs em risco, inativos)
- Deploy no Streamlit Cloud
- Workflow GitHub Actions para sincronizacao automatica de dados

### Sessao 2 (26-27/02/2026) — Melhorias e Ajustes
- Filtro de lojas cascading por segmento
- Multi-selecao de periodos com agregacao (meses 2025/2026)
- Correcao botao Sair invisivel no sidebar escuro
- Pizza mostra concentracao de valor (nao contagem)
- KPIs e graficos respondem aos filtros
- Grafico de lojas filtra por segmento/loja
- Reestruturacao colunas: +Cliente_ID, +Primeiro_Nome, +Valor_Total_Filtrado, -CPF
- Analise de upgrade por perfil (Score_Total_RFV, gap <= 2 pontos)
- Pagina AJFANS (3 tabs: Distribuicao, Ranking, Lista)

### Sessao 3 (27/02/2026) — Explicacoes + Visual + Fixes
- Textos explicativos em todas as secoes (st.info, st.caption, help tooltips)
- Correcao erro cabecalhos duplicados Google Sheets (ler_aba_como_df)
- Cabecalhos automaticos corretos em todas as abas admin
- **Remodelacao visual completa** com identidade AJ (Manual House Materiais):
  - Azul Navy #031835, Cinza Gelo #dfe2e6
  - Cores dinamicas por shopping (accent, accent2, chart sequence)
  - Tipografia brand (thin, letter-spaced)
  - Perfis com cores sofisticadas (ouro antigo, prata, bronze, slate)

---

## Notificacoes WhatsApp (Sessao 03/03/2026)

### Script `scripts/notificar_whatsapp.py`
- Script reutilizavel para qualquer dashboard (parametro `--dashboard`)
- Envia via WhatsApp Gateway local (Baileys, localhost:3001)
- Mensagens: sucesso (🛍 + detalhes + link dashboard) e erro (❌ + link Actions)

### GitHub Actions (atualizado)
- **Job principal:** `sincronizar` roda no `ubuntu-latest` (cloud)
- **Job notificacao:** `notificar-whatsapp` roda no `self-hosted` (acesso ao gateway)
- **Condicao:** `if: always()` + `needs.sincronizar.result` para sucesso/erro
- **Detalhes:** Inclui contagem de CSVs sincronizados na mensagem

### Sessao 4 (03/03/2026) — Notificacoes WhatsApp
- Adicionado script notificar_whatsapp.py (reutilizavel entre projetos)
- Job de notificacao WhatsApp no workflow (self-hosted runner)
- Mensagens com emojis: sucesso + erro

---

## Proximos Passos Sugeridos

1. Considerar Git LFS para ranking_ajfans.csv (88MB, proximo do limite)
2. Adicionar mais periodos conforme dados ficam disponiveis
3. Implementar comparativo entre periodos (tendencia)

---

*Ultima atualizacao: 03/03/2026 (Sessao 4)*
*Repositorio: https://github.com/carlosgravi/shopping-relacionamento*
