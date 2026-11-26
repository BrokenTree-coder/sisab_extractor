# Extrator e Pipeline ETL do SISAB (Produção em Saúde)

Coletor automatizado em Python + Playwright e pipeline ETL (`E1`–`E5`) para extração, padronização, agregação, validação e relato da série histórica de atendimentos individuais por município no portal **SISAB (Sistema de Informação em Saúde para a Atenção Básica)**, do Ministério da Saúde, alinhado à arquitetura do projeto **Rede SOFIA (`pipeline_sofia/sisab/`)**.

---

## 1. Contexto e Motivação

O portal do SISAB disponibiliza dados públicos e agregados de produção da Atenção Primária à Saúde (`RelSauProducao.xhtml`). Apesar de ricos para análises epidemiológicas, extrair e tratar séries históricas longas manualmente pela interface web traz gargalos operacionais recorrentes:

- **Volume repetitivo:** uma janela de abril/2013 a outubro/2026 soma **163 competências mensais por agravo**. Para 6 módulos epidemiológicos distintos, são **978 consultas individuais**.
- **Nome de arquivo sempre igual:** toda exportação do portal baixa o arquivo com o mesmo nome genérico (`RelatorioSaudeProducao.csv`), exigindo renomeação imediata para evitar sobrescrita e perda de rastreabilidade.
- **Limites técnicos da aplicação JSF/JBoss:** a página do relatório possui ~6 MB de HTML (carregando municípios e tabelas CID/CIAP no DOM) e impõe um limite interno de servidor (`maxParameterCount = 512`). Consultar filtrando múltiplos municípios por UF estoura esse limite em estados grandes; aqui o coletor contorna o problema usando **`Unidade Geográfica = Brasil`** com **`Linha = Município`** e **`Coluna = CID`**, reduzindo a requisição para cerca de ~42 parâmetros.
- **Filtros secundários abertos:** os filtros opcionais da tela (`Tipo de Equipe`, `Categoria do Profissional`, `Sexo`, `Local de Atendimento` e `Tipo de Atendimento`) permanecem sem marcação (`Nada selecionado`), e `Faixa Etária` permanece em `Ignorar`. No SISAB, isso evita restrições adicionais (retornando a totalidade dos registros daquele CID na competência) e impede o aumento desnecessário de parâmetros no `POST`.
- **Meses sem registros silenciosos:** quando uma competência não tem casos registrados, o SISAB não baixa um CSV vazio — ele retorna **HTTP `204` (`No Content`)** e recarrega o formulário sem disparar download.
- **Cabeçalhos, totais e rodapés nos CSVs brutos:** cada exportação inclui blocos de título/filtros no topo, `;` excedente ao final das linhas e rodapés oficiais (`Fonte:` e `Dado gerado em:`), que precisam ser removidos exclusivamente por script para preservar a camada bruta intocada.

### Automação controlada ("até certo ponto")

O objetivo deste projeto não é fazer *scraping* agressivo nem sobrecarregar a infraestrutura do Ministério da Saúde, mas substituir o clique manual repetitivo por um fluxo **sequencial, auditável e retomável**:

- **1 único worker sequencial** reaproveitando a sessão do navegador, com pausas aleatórias (*jitter* de `2,5s` a `5,0s`) entre consultas para manter um ritmo próximo ao humano.
- **Resiliência de rede:** renovação preventiva de sessão JSF (a cada 40 consultas), *backoff* exponencial (`15s` → `45s` → `120s`) com novo contexto limpo (`JSESSIONID`) em caso de falha e **disjuntor (*circuit breaker*)** que interrompe a execução após 3 competências seguidas com erro.
- **Camada bronze imutável e rastreável por lote:** cada CSV é auditado antes de ser salvo em `data/bronze/`, preservando cabeçalho de filtros e rodapé oficial sob uma convenção com identificador sequencial de lote (`J<NNNN>`), alimentando diretamente as etapas `E1` a `E5` do pipeline ETL.

---

## 2. Estrutura do Projeto

```text
sisab_extractor/
├── .gitignore
├── README.md
├── main.py                              # cli do extrator automatizado (playwright)
├── config/
│   └── catalogo_morbidades.json         # catalogo declarativo (url, eixos, periodo e cids)
├── src/
│   ├── __init__.py
│   ├── auditor.py                       # valida cabecalho, competencia, cids e rodape dos csvs brutos
│   ├── checkpoint.py                    # banco sqlite com estado atomico, lotes Jnnnn e retomada
│   └── scraper.py                       # motor playwright com reuso de sessao, deteccao 204 e protecoes
├── etl/
│   ├── __init__.py
│   ├── e1_conferir_bronze.py            # E1: arquivo x registro, integridade e sha-256
│   ├── e2_padronizar.py                 # E2: remove cabecalho/rodape e gera silver_sisab_valor (longo)
│   ├── e3_agregar.py                    # E3: gera gold_sisab_municipio_ano e gold_sisab_uf_ano (largo)
│   ├── e4_validar.py                    # E4: validacoes V1, V2, V4 (ibge), V5 e V6 (opcional)
│   ├── e5_relatar.py                    # E5: manifesto json da execucao e mapa de cobertura
│   └── rodar_etl.py                     # orquestrador unico das etapas E1 a E5
├── data/
│   ├── checkpoint.db                    # estado local do coletor (ignorado no git)
│   ├── bronze/                          # csvs brutos validados e imutaveis, separados por tema
│   │   ├── sifg/                        # SISAB_sifg_<AAAAMM>_J0001.csv
│   │   ├── hepa/                        # SISAB_hepa_<AAAAMM>_J0002.csv
│   │   ├── src/                         # SISAB_src_<AAAAMM>_J0003.csv
│   │   ├── tetn/                        # SISAB_tetn_<AAAAMM>_J0004.csv
│   │   ├── toxc/                        # SISAB_toxc_<AAAAMM>_J0005.csv
│   │   └── sifc/                        # SISAB_sifc_<AAAAMM>_J0006.csv
│   ├── silver/
│   │   ├── silver_sisab_valor.csv       # tabela longa padronizada (mun, uf, ano, mes, quadrimestre, medida, valor)
│   │   └── relatorio_e2_silver.json     # resumo da padronizacao E2
│   └── gold/
│       ├── gold_sisab_municipio_ano.csv # painel anual municipal (um indicador por coluna)
│       ├── gold_sisab_uf_ano.csv        # painel anual estadual (um indicador por coluna)
│       ├── cobertura_v5_por_tema.csv    # cobertura de competencias, municipios e celulas (V5)
│       ├── mapa_cobertura_uf_ano.csv    # mapa detalhado tema x uf x ano (E5)
│       ├── mapa_cobertura_sisab.md      # resumo visual da cobertura temporal e territorial (E5)
│       ├── relatorio_e3_gold.json       # resumo e hashes da agregacao E3
│       ├── relatorio_e4_validacao.json  # resultado estruturado das validacoes E4
│       └── manifestos/
│           └── manifesto_YYYYMMDD_HHMMSS.json # manifesto padrao pipeline_sofia (E5)
├── registro/
│   ├── registro_bronze.csv              # registro consolidado por lote (J0001..J0006)
│   ├── manifesto_bronze.csv             # inventario por arquivo com sha-256 e contagem de linhas (E1)
│   └── relatorio_e1_bronze.json         # relatorio de conciliacao da etapa E1
├── referencias/
│   └── municipios_ibge.csv              # tabela oficial de 5.571 municipios (API Localidades IBGE)
├── docs/                                # inventario de relatorios e documentacao complementar
├── analises/                            # scripts e notebooks exploratorios
└── tests/                               # testes automatizados
```

### Convenção de nomenclatura dos arquivos

- **Camada bronze (extrato mensal individual em `data/bronze/<tema>/`):**
  `SISAB_<tema>_<AAAAMM>_<JNNNN>.csv`
  Exemplo: `data/bronze/sifg/SISAB_sifg_201405_J0001.csv`
- **Identificador de lote (`J<NNNN>` / `L<NNNN>`):** sequência global (`J0001`, `J0002`, ...) atribuída uma única vez por módulo na tabela `lotes_consulta` do SQLite. Como o prefixo (`J`/`L`) já identifica a autoria de cada consulta/lote, não há subpasta de pessoa dentro de `data/bronze/`.
- **Referência do lote consolidado (`registro/registro_bronze.csv`):** mantém uma linha por lote (`J0001` a `J0006`) com o identificador de série `SISAB_<tema>_<AAAAMM_INICIAL>_<AAAAMM_FINAL>_<JNNNN>.csv`. O cálculo de `sha256` individual por CSV e do `sha256_lote` determinístico é gerado automaticamente em `registro/manifesto_bronze.csv` e `registro/relatorio_e1_bronze.json` pela etapa `E1`.

### Responsabilidade de cada componente

- **`config/catalogo_morbidades.json`** — Concentra 100% da regra epidemiológica (módulos, códigos CID/CIAP e janela temporal). O extrator não tem nenhuma doença fixa (*hardcoded*).
- **`main.py` e `src/` (`scraper.py`, `auditor.py`, `checkpoint.py`)** — Camada de coleta automatizada via Playwright com auditoria imediata em `latin-1` (competência exclusiva, códigos CID/CIAP exatos e rodapé `Fonte:`) e persistência atômica no `data/checkpoint.db` (SQLite em modo `WAL`).
- **`etl/` (`e1` a `e5` + `rodar_etl.py`)** — Camada analítica e de governança desacoplada do coletor (capaz de ler tanto lotes automatizados `J` quanto eventuais coletas manuais `L` na `bronze`), escrita apenas com a biblioteca padrão do Python (com suporte opcional a `pandas`/`pyarrow` para exportação `.parquet`).

---

## 3. Configuração do `config/catalogo_morbidades.json` (Obrigatório)

**O coletor só consegue extrair uma doença se ela estiver explicitamente mapeada neste arquivo.** Todo o comportamento do *scraper* (quais checkboxes marcar no modal do SISAB) e do *auditor* (quais códigos exigir no cabeçalho do CSV baixado) é guiado diretamente pelo JSON.

### Anatomia do arquivo

```json
{
  "portal": {
    "url": "[https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal/saude/RelSauProducao.xhtml](https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal/saude/RelSauProducao.xhtml)",
    "linha": "MUN.CO_MUNICIPIO_IBGE",
    "coluna": "CID",
    "unidade_geografica": "brasil"
  },
  "periodo": {
    "inicio": "2013-04",
    "fim": "2026-10"
  },
  "modulos": {
    "sifg": {
      "nome": "Sifilis em Gestante",
      "codigos": [
        { "sistema": "CID", "codigo": "O981" }
      ]
    },
    "sifc": {
      "nome": "Sifilis Congenita",
      "codigos": [
        { "sistema": "CID", "codigo": "A50" },
        { "sistema": "CID", "codigo": "A500" },
        { "sistema": "CID", "codigo": "A501" },
        { "sistema": "CID", "codigo": "A502" },
        { "sistema": "CID", "codigo": "A503" },
        { "sistema": "CID", "codigo": "A504" },
        { "sistema": "CID", "codigo": "A505" },
        { "sistema": "CID", "codigo": "A506" },
        { "sistema": "CID", "codigo": "A507" },
        { "sistema": "CID", "codigo": "A509" }
      ]
    }
  }
}
```

### Regras para adicionar ou editar uma morbidade

1. **Chave do módulo (ex.: `"sifg"`):** vira o argumento aceito em `--modulo`, o nome da subpasta em `data/bronze/<modulo>/` e o identificador nos CSVs gerados (`SISAB_<modulo>_<AAAAMM>_<JNNNN>.csv`). Use nomes curtos, sem espaços ou acentos.
2. **Lista `"codigos"`:** cada entrada precisa informar obrigatoriamente `"sistema"` (`"CID"` ou `"CIAP"`) e `"codigo"` sem pontos (ex.: `O981`, `A500`, `P371`).
3. **Subcategorias em árvore:** no SISAB, marcar um código pai (como `A50`) **não** marca automaticamente seus filhos (`A500`–`A509`). Todos os subcódigos devem ser listados individualmente.
4. **Janela `"periodo"`:** formato ordenável `AAAA-MM` (convertido internamente para `MMM/AAAA` no portal).

### Módulos mapeados no catálogo padrão

| Módulo | Agravo | Códigos mapeados |
| :--- | :--- | :--- |
| `sifg` | Sífilis em Gestante | `CID: O981` |
| `hepa` | Hepatites Virais | `CID: O984` |
| `sifc` | Sífilis Congênita | `CID: A50, A500, A501, A502, A503, A504, A505, A506, A507, A509` |
| `src` | Síndrome da Rubéola Congênita | `CID: P350` |
| `tetn` | Tétano Neonatal | `CID: A33` |
| `toxc` | Toxoplasmose Congênita | `CID: P371` |

---

## 4. Pipeline ETL (`E1` a `E5`) e Validações (`V1` a `V6`)

O pipeline segue o desenho padrão do `pipeline_sofia`:

| Etapa | Script | Entrada | Saída Principal | O que faz |
| :---: | :--- | :--- | :--- | :--- |
| **E1** | `etl/e1_conferir_bronze.py` | `data/bronze/` + `registro/registro_bronze.csv` (+ `checkpoint.db` opcional) | `registro/manifesto_bronze.csv`, `registro/relatorio_e1_bronze.json` | Confere 100% de correspondência arquivo × registro, valida cabeçalho/rodapé dos CSVs e calcula `sha256` por arquivo e `sha256_lote`. |
| **E2** | `etl/e2_padronizar.py` | `data/bronze/` + `registro/registro_bronze.csv` | `data/silver/silver_sisab_valor.csv`, `data/silver/relatorio_e2_silver.json` | Remove título, filtros, linhas `Total` e rodapé por script; normaliza `mun` (IBGE 6 dígitos), `uf`, `ano`, `mes`, `quadrimestre` (`Q1`–`Q3`) e despivota colunas de CID para o formato longo (`medida`, `valor`), omitindo células zeradas/vazias por padrão. |
| **E3** | `etl/e3_agregar.py` | `data/silver/silver_sisab_valor.csv` | `data/gold/gold_sisab_municipio_ano.csv`, `data/gold/gold_sisab_uf_ano.csv`, `data/gold/relatorio_e3_gold.json` | Agrega a `silver` em painéis anuais largos com **um indicador por coluna** (`hepa, sifc, sifg, src, tetn, toxc`), mantendo mês e quadrimestre na `silver`, e valida conservação exata das somas (`silver == gold_mun == gold_uf`). |
| **E4** | `etl/e4_validar.py` | `bronze`, `silver`, `gold` + `referencias/municipios_ibge.csv` | `data/gold/relatorio_e4_validacao.json`, `data/gold/cobertura_v5_por_tema.csv` | Executa as validações `V1`, `V2`, `V4`, `V5` e `V6` (opcional). Na primeira execução, baixa automaticamente os 5.571 municípios oficiais da API de Localidades do IBGE em `referencias/municipios_ibge.csv`. |
| **E5** | `etl/e5_relatar.py` | Relatórios `E1`–`E4`, `bronze`, `silver`, `gold` | `data/gold/manifestos/manifesto_YYYYMMDD_HHMMSS.json`, `data/gold/mapa_cobertura_uf_ano.csv`, `data/gold/mapa_cobertura_sisab.md` | Reaproveita a estrutura de `pipeline_sofia/d1_novo/manifesto.py` (commit Git, versões do ambiente, resumo das entradas, resultados e falhas) e gera os mapas de cobertura temporal e territorial. |

### Esquema das camadas padronizadas

- **Silver (`data/silver/silver_sisab_valor.csv` — formato longo):**
  `id_consulta, relatorio, tema, filtros, nivel, mun, uf, ano, mes, quadrimestre, medida, valor`
- **Gold municipal (`data/gold/gold_sisab_municipio_ano.csv` — formato largo):**
  `mun, uf, ano, hepa, sifc, sifg, src, tetn, toxc`
- **Gold estadual (`data/gold/gold_sisab_uf_ano.csv` — formato largo):**
  `uf, ano, hepa, sifc, sifg, src, tetn, toxc`

### Critérios de validação da Etapa E4

- **V1 (Registro completo):** 100% dos arquivos da `bronze` vinculados a um `id_consulta` do registro e 100% dos lotes com arquivos em disco (sem órfãos).
- **V2 (Soma territorial):** conservação exata das somas em toda a cadeia (`bronze == silver == soma(municípios) == gold_uf == Brasil`), além da checagem contra eventuais linhas `Total` dos CSVs brutos.
- **V3 (Reprodução cruzada):** ignorada no escopo automatizado atual (registrada como `ignorado` no relatório JSON).
- **V4 (Chave válida IBGE):** 100% dos códigos `mun` de 6 dígitos presentes em `referencias/municipios_ibge.csv` e coerentes com a `uf` oficial do município.
- **V5 (Cobertura):** contabiliza por tema as competências com/sem valor, municípios distintos alcançados e esparsidade da grade `municípios × competências`.
- **V6 (Comparação com outra base — opcional):** ativada sob demanda via `--base-externa <csv> --v6-tema <tema>` para comparar `gold_sisab_uf_ano.csv` com bases externas (ex.: SINAN ou SINASC) por UF e ano, calculando razões e sinalizando desvios em `data/gold/comparacao_v6_externa.csv`.

---

## 5. Instalação e Dependências (`.venv`)

O projeto requer **Python 3.10+**.

- **Pipeline ETL (`etl/`):** utiliza apenas a biblioteca padrão do Python (`csv`, `json`, `hashlib`, `sqlite3`, `urllib`, `statistics`, `argparse`, `pathlib`). `pandas` e `pyarrow` só são necessários caso você use a flag opcional `--parquet`.
- **Extrator (`main.py` / `src/`):** requer **Playwright** e o binário do navegador Chromium.

### Passo a passo (Linux / WSL / macOS)

```bash
# clona o repositorio e entra na pasta
git clone <url-do-repositorio>
cd sisab_extractor

# cria e ativa o ambiente virtual isolado
python3 -m venv .venv
source .venv/bin/activate

# instala o playwright e o binario do chromium (necessario para a extracao)
pip install --upgrade pip
pip install playwright
playwright install chromium
```

> **Linux/WSL:** se o Chromium acusar falta de bibliotecas de sistema ao iniciar, execute `playwright install-deps chromium`.

### Passo a passo (Windows PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install playwright
playwright install chromium
```

---

## 6. Comandos de Uso

Todos os comandos abaixo devem ser executados a partir da raiz do projeto com o ambiente virtual ativo.

### A. Extração automatizada (Camada Bronze)

```bash
# extrai toda a serie historica configurada no catalogo para um modulo
python main.py --modulo sifg

# executa todos os modulos do catalogo em sequencia
python main.py --modulo todos

# baixa apenas um intervalo especifico de competencias
python main.py --modulo sifg --inicio 2026-07 --fim 2026-09

# executa no maximo 3 competencias pendentes com o navegador visivel (teste de modulo)
python main.py --modulo sifc --limite 3 --headed

# exibe o resumo consolidado do checkpoint (downloaded, no_data e error por modulo)
python main.py --status

# valida manualmente um csv especifico da bronze contra o catalogo
python src/auditor.py data/bronze/sifg/SISAB_sifg_202609_J0001.csv sifg 2026-09
```

> Se a coleta for interrompida (`Ctrl+C` ou queda de rede), basta repetir o mesmo comando: o `data/checkpoint.db` reaproveita o lote `J<NNNN>`, pula competências já concluídas e retoma da primeira pendente. Para cargas históricas completas, prefira rodar à noite ou em fins de semana, quando o portal do SISAB oscila menos.

### B. Execução do Pipeline ETL (`E1` a `E5`)

```bash
# executa o pipeline completo (E1 -> E2 -> E3 -> E4 -> E5) em uma unica chamada
python etl/rodar_etl.py

# executa apenas um subconjunto de etapas
python etl/rodar_etl.py --etapas E3 E4 E5

# gera tambem copias em parquet na silver e na gold
python etl/rodar_etl.py --parquet

# executa incluindo a comparacao opcional V6 contra uma base externa (ex.: SINAN/SINASC)
python etl/rodar_etl.py --base-externa caminho/para/base_uf_ano.csv --v6-tema sifg --v6-col-valor valor
```

Também é possível rodar qualquer etapa isoladamente:

```bash
python etl/e1_conferir_bronze.py
python etl/e2_padronizar.py               # aceita --manter-zeros e --parquet
python etl/e3_agregar.py                  # aceita --prefixo-indicador sisab_ e --parquet
python etl/e4_validar.py                  # aceita --baixar-municipios e --base-externa
python etl/e5_relatar.py
```

---

## 7. Resultados Reais da Coleta e do Pipeline (`2013-04` a `2026-10`)

Todas as 6 morbidades catalogadas foram extraídas integralmente na janela de `2013-04` a `2026-10` (**163 competências por módulo**, totalizando **978 consultas** fechadas com **0 erros finais**) e processadas de ponta a ponta pelo `etl/rodar_etl.py` em **3,79 segundos**.

### Consolidação por módulo (`Bronze` → `Silver` → `Gold` / `V5`)

| Módulo | Lote | Agravo | CSVs (`downloaded`) | `no_data` | Linhas Bronze | Linhas Silver | Atendimentos Somados | Municípios Únicos (`%` de 5.571) | Primeira – Última Comp. |
| :--- | :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `sifg` | `J0001` | Sífilis em Gestante | 150 | 13 | 32.654 | 32.654 | 86.392 | 3.625 (65,1%) | `2014-05` – `2026-10` |
| `hepa` | `J0002` | Hepatites Virais | 137 | 26 | 977 | 977 | 1.295 | 440 (7,9%) | `2014-09` – `2026-10` |
| `src` | `J0003` | Síndrome da Rubéola Congênita | 146 | 17 | 3.512 | 3.512 | 4.656 | 886 (15,9%) | `2014-01` – `2026-10` |
| `tetn` | `J0004` | Tétano Neonatal | 84 | 79 | 132 | 132 | 167 | 89 (1,6%) | `2014-06` – `2026-09` |
| `toxc` | `J0005` | Toxoplasmose Congênita | 148 | 15 | 19.453 | 19.453 | 31.294 | 3.130 (56,2%) | `2014-02` – `2026-10` |
| `sifc` | `J0006` | Sífilis Congênita | 153 | 10 | 40.993 | 53.603 | 117.815 | 4.234 (76,0%) | `2013-11` – `2026-10` |
| **Total** | — | **6 morbidades** | **818** | **160** | **97.721** | **110.331** | **241.619** | **4.946 únicos (88,8%)** | **`2013-11` – `2026-10`** |

### Síntese das saídas e validações do pipeline (`E1`–`E5`)

- **E1 (`0,531s`):** `818/818` CSVs da bronze conciliados com os `6/6` lotes do registro (`97.721` linhas municipais auditadas, 0 erros).
- **E2 (`1,166s`):** `110.331` linhas geradas em `silver_sisab_valor.csv`, `356.327` células zeradas omitidas nos módulos multi-CID (`sifc`) e `0` valores inválidos.
- **E3 (`0,502s`):** `gold_sisab_municipio_ano.csv` com `24.565` linhas (`4.946` municípios únicos) e `gold_sisab_uf_ano.csv` com `344` linhas (`27` UFs, anos `2013` a `2026`), com 100% de conservação das somas (`241.619` atendimentos).
- **E4 (`1,118s`):**
  - `V1` (registro completo): **aprovado** (`818/818` arquivos, `6/6` lotes).
  - `V2` (soma territorial): **aprovado** (`0` divergências entre bronze, silver, municípios, UFs e Brasil).
  - `V3` (reprodução cruzada): `ignorado`.
  - `V4` (chave válida IBGE): **aprovado** (`4.946/4.946` municípios = `100,00%` válidos contra os `5.571` municípios de `referencias/municipios_ibge.csv`).
  - `V5` (cobertura): **informado** (`95,49%` a `99,99%` de células `município × mês` sem ocorrência na grade de `908.073` pares por tema, padrão esperado para agravos específicos em granularidade mensal municipal).
  - `V6` (base externa): `nao_executado` (disponível sob demanda via `--base-externa`).
- **E5 (`0,472s`):** `2.268` linhas geradas em `data/gold/mapa_cobertura_uf_ano.csv` (`6 temas × 14 anos × 27 UFs`), resumo em `data/gold/mapa_cobertura_sisab.md` e manifesto consolidado em `data/gold/manifestos/` com `0` falhas.

### Por que meses `no_data` demoram bem mais que meses com dados na extração?

Esse comportamento decorre da arquitetura do próprio portal SISAB:

- **Quando há dados (`downloaded`):** o servidor devolve diretamente o fluxo `text/csv` (HTTP `200`) na sessão já aberta, sem recarregar a página — em média entre `3,5 s` e `6,0 s` por competência. No módulo mais volumoso (`sifc`, com 10 códigos CID e `40.993` linhas municipais na bronze / `53.603` na silver em 153 meses), o lote inteiro fechou em cerca de **13 a 14 minutos** de relógio.
- **Quando não há dados (`no_data`):** além da resposta HTTP `204`, o *postback* JSF do SISAB força o recarregamento completo da página (~6 MB de HTML), e o coletor precisa aguardar o DOM estabilizar e verificar se os filtros sobreviveram (recriando a sessão se o formulário tiver sido limpo). Por isso, agravos mais raros — como `tetn` (`CID A33`), com 79 competências `no_data` — levaram cerca de **37 a 39 minutos** mesmo gerando menos arquivos em disco.

### Trecho real do log da extração (`sifc` — lote `J0006`)

```text
[sifc] iniciando extracao de 163 competencia(s) (lote J0006, total na janela: 163).
[sifc] (1/163) 2013-04: no_data em 6.71s
[sifc] (2/163) 2013-05: no_data em 6.66s
...
[sifc] (8/163) 2013-11: downloaded (1 registros) em 1.71s
[sifc] (9/163) 2013-12: no_data em 6.84s
...
[sifc] renovando sessao jsf preventiva...
[sifc] (53/163) 2017-08: downloaded (186 registros) em 2.64s
...
[sifc] (162/163) 2026-09: downloaded (493 registros) em 4.78s
[sifc] (163/163) 2026-10: downloaded (486 registros) em 4.80s
resumo do checkpoint:
  sifc         | downloaded | 153
  sifc         | no_data    | 10
```
