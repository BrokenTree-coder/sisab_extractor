# Extrator Automatizado do SISAB (Produção em Saúde)

Coletor automatizado em Python + Playwright para extração em lote da série histórica de atendimentos individuais por município no portal **SISAB (Sistema de Informação em Saúde para a Atenção Básica)**, do Ministério da Saúde.

---

## 1. Contexto e Motivação

O portal do SISAB disponibiliza dados públicos e agregados de produção da Atenção Primária à Saúde (`RelSauProducao.xhtml`). Apesar de ricos para análises epidemiológicas, extrair séries históricas longas manualmente pela interface web traz vários gargalos operacionais:

* **Volume repetitivo:** uma janela de abril/2013 a outubro/2026 soma **163 competências mensais por agravo**. Para 6 módulos epidemiológicos distintos, são até **978 consultas individuais**.
* **Nome de arquivo sempre igual:** toda exportação do portal baixa o arquivo com o mesmo nome genérico (`RelatorioSaudeProducao.csv`), exigindo renomeação manual imediata para evitar sobrescrita e perda de rastreabilidade.
* **Limites técnicos da aplicação JSF/JBoss:** a página do relatório possui ~6 MB de HTML (carregando municípios e tabelas CID/CIAP no DOM) e impõe um limite interno de servidor (`maxParameterCount = 512`). Consultar filtrando múltiplos municípios por UF estoura esse limite em estados grandes; aqui o coletor contorna o problema usando **`Unidade Geográfica = Brasil`** com **`Linha = Município`** e **`Coluna = CID`**, reduzindo a requisição para cerca de ~42 parâmetros.
* **Meses sem registros silenciosos:** quando uma competência não tem casos registrados, o SISAB não baixa um CSV vazio — ele retorna **HTTP `204` (`No Content`)** e recarrega o formulário sem disparar download.

### Automação controlada ("até certo ponto")

O objetivo deste projeto não é fazer *scraping* agressivo nem sobrecarregar a infraestrutura do Ministério da Saúde, mas sim substituir o clique manual repetitivo por um fluxo **sequencial, auditável e retomável**:

* **1 único worker sequencial** reaproveitando a sessão do navegador, com pausas aleatórias (*jitter* de `2,5s` a `5,0s`) entre consultas para manter um ritmo próximo ao humano.
* **Resiliência de rede:** renovação preventiva de sessão JSF (a cada 40 consultas), *backoff* exponencial (`15s` → `45s` → `120s`) com novo contexto limpo (`JSESSIONID`) em caso de falha e **disjuntor (*circuit breaker*)** que interrompe a execução após 3 competências seguidas com erro.
* **Camada bruta intocada:** cada CSV é auditado antes de ser salvo em disco, preservando cabeçalho de filtros e rodapé oficial (`Fonte:` e `Dado gerado em:`) para rastreabilidade antes de qualquer etapa posterior de agregação.

---

## 2. Estrutura do Projeto

```text
sisab_extractor/
├── config/
│   └── catalogo_morbidades.json   # catalogo declarativo com url, eixos, periodo e cids por modulo
├── src/
│   ├── __init__.py
│   ├── auditor.py                 # valida cabecalho, competencia, cids e rodape dos csvs brutos
│   ├── checkpoint.py              # banco sqlite com estado atomico e retomada automatica
│   └── scraper.py                 # motor playwright com reuso de sessao, deteccao 204 e protecoes
├── data/
│   ├── checkpoint.db              # gerado automaticamente na primeira execucao
│   └── raw/                       # csvs brutos validados, separados em pastas por modulo
│       ├── sinan-sifg/
│       │   ├── sinan-sifg_2014-05.csv
│       │   └── ...
│       ├── sinan-hepa/
│       ├── sinan-sifc/
│       ├── sinan-src/
│       ├── sinan-tetn/
│       └── sinan-toxc/
└── main.py                        # interface de linha de comando (cli) que orquestra tudo
```

### O que acontece em cada arquivo

* **`config/catalogo_morbidades.json`** — Concentra 100% da regra epidemiológica (módulos, códigos CID/CIAP e janela temporal). O código Python não tem nenhuma doença fixa (*hardcoded*); ele apenas executa o que está declarado neste JSON.
* **`main.py`** — Ponto de entrada via terminal. Lê o catálogo, gera a lista cronológica de competências (`AAAA-MM`), instancia o banco de *checkpoint* e chama o `SisabScraper` para o(s) módulo(s) escolhido(s).
* **`src/scraper.py`** — Controla o Chromium via Playwright:
  1. Abre a página do SISAB, fixa `Linha`, `Coluna` e `Unidade Geográfica` e marca os códigos do módulo no modal `CIAP/CID` usando regex exata (`^CID \(CODIGO\)`), evitando marcar códigos pais ou equivalentes `CIAP` por engano.
  2. Para cada competência pendente, desmarca o mês anterior, marca o mês atual e clica em `Download > Csv`.
  3. Monitora os eventos de rede da aba: se o servidor responder **HTTP `204`**, encerra a espera e registra `no_data`; se o download iniciar, grava primeiro em um arquivo temporário (`.csv.tmp`).
  4. Só move o `.csv.tmp` para o nome final determinístico (`<modulo>_AAAA-MM.csv`) se ele passar integralmente na auditoria.
* **`src/auditor.py`** — Proteção contra erros silenciosos de estado do JSF (como o portal devolver o CSV do mês anterior com o nome do mês atual) e contra arquivos truncados. Lê o arquivo em `latin-1`, verifica se a linha `Competência:` contém **exclusivamente** o mês pedido, confirma se `CIAP/CID:` bate exatamente com o catálogo, extrai os códigos das colunas truncadas da tabela via regex, trata o `;` sobrando no final de cada linha e exige a presença do rodapé `Fonte:`.
* **`src/checkpoint.py`** — Gerencia o `data/checkpoint.db` (SQLite em modo `WAL`, resistente a interrupções com `Ctrl+C`). Registra cada par `(modulo, competencia)` como `downloaded`, `no_data` ou `error`, além da contagem de registros e data de geração. Antes de pular um mês `downloaded`, também confirma se o `.csv` correspondente ainda existe no disco.

---

## 3. Configuração do `config/catalogo_morbidades.json` (Obrigatório)

**O coletor só consegue extrair uma doença se ela estiver explicitamente mapeada neste arquivo.** Todo o comportamento do *scraper* (quais checkboxes marcar no modal do SISAB) e do *auditor* (quais códigos exigir no cabeçalho do CSV baixado) é guiado diretamente pelo JSON.

Se um código não estiver listado corretamente — ou se o prefixo `CID` vs. `CIAP` estiver errado — o coletor interromperá a execução na validação em vez de baixar dados inconsistentes.

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
    "sinan-sifg": {
      "nome": "Sifilis em Gestante",
      "codigos": [
        { "sistema": "CID", "codigo": "O981" }
      ]
    },
    "sinan-sifc": {
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

1. **Chave do módulo (ex.: `"sinan-sifg"`):** vira o argumento aceito em `--modulo`, o nome da subpasta em `data/raw/<modulo>/` e o prefixo dos CSVs gerados (`<modulo>_AAAA-MM.csv`). Use nomes curtos, sem espaços ou acentos.
2. **Lista `"codigos"`:** cada entrada precisa informar obrigatoriamente:
   * `"sistema"`: `"CID"` ou `"CIAP"` (define o prefixo buscado no modal do SISAB).
   * `"codigo"`: o código exato **sem pontos** (ex.: `O981`, `A500`, `P371`), exatamente como aparece entre parênteses no buscador do portal (ex.: `CID (O981)`).
3. **Subcategorias em árvore:** no SISAB, marcar um código pai (como `A50`) **não** marca automaticamente seus filhos (`A500`–`A509`). Se a morbidade englobar subcódigos, todos precisam ser listados um a um em `"codigos"`, como no exemplo de `sinan-sifc`.
4. **Janela `"periodo"`:** usa sempre o formato ordenável `AAAA-MM` (o script converte internamente para o formato `MMM/AAAA` exigido pelo portal).

### Módulos já mapeados no catálogo padrão

| Módulo | Agravo | Códigos mapeados |
| :--- | :--- | :--- |
| `sinan-sifg` | Sífilis em Gestante | `CID: O981` |
| `sinan-hepa` | Hepatites Virais | `CID: O984` |
| `sinan-sifc` | Sífilis Congênita | `CID: A50, A500, A501, A502, A503, A504, A505, A506, A507, A509` |
| `sinan-src` | Síndrome da Rubéola Congênita | `CID: P350` |
| `sinan-tetn` | Tétano Neonatal | `CID: A33` |
| `sinan-toxc` | Toxoplasmose Congênita | `CID: P371` |

---

## 4. Instalação e Dependências (`.venv`)

O projeto requer **Python 3.10+** (devido à sintaxe de tipagem `str | None`). Quase toda a base usa apenas a biblioteca padrão do Python (`sqlite3`, `json`, `re`, `argparse`, `pathlib`); a única dependência externa é o **Playwright** e o binário do navegador Chromium.

### Passo a passo (Linux / WSL / macOS)

```bash
# clona o repositorio e entra na pasta
git clone <url-do-repositorio>
cd sisab_extractor

# cria e ativa o ambiente virtual isolado
python3 -m venv .venv
source .venv/bin/activate

# instala o playwright e o binario do chromium
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

## 5. Comandos de Uso

Todos os comandos abaixo devem ser executados a partir da raiz do projeto com o `.venv` ativo.

### Execução padrão (recomendado: 1 módulo por vez)

```bash
# extrai toda a serie historica configurada no catalogo para um modulo
python main.py --modulo sinan-sifg

# executa todos os modulos do catalogo em sequencia
python main.py --modulo todos
```

Se você interromper a execução com `Ctrl+C` ou ocorrer queda de internet, basta rodar o **mesmo comando novamente**: o `data/checkpoint.db` pula automaticamente todas as competências já gravadas como `downloaded` ou `no_data` e retoma da primeira pendente.

### Recortes de período, testes e inspeção visual

```bash
# baixa apenas um intervalo especifico de competencias
python main.py --modulo sinan-sifg --inicio 2026-07 --fim 2026-09

# executa no maximo 3 competencias pendentes com o navegador visivel (util para validar um modulo novo)
python main.py --modulo sinan-sifc --limite 3 --headed

# ajusta o timeout maximo (em segundos) aguardando inicio do download (padrao: 60s)
python main.py --modulo sinan-hepa --timeout-download 60
```

### Acompanhamento de progresso e auditoria manual

```bash
# exibe o resumo consolidado do checkpoint (downloaded, no_data e error por modulo)
python main.py --status

# exibe o status apenas de um modulo especifico
python main.py --modulo sinan-sifg --status

# valida manualmente um csv especifico contra o catalogo
python src/auditor.py data/raw/sinan-sifg/sinan-sifg_2026-09.csv sinan-sifg 2026-09

# confere quantidade de arquivos e espaco ocupado em disco
ls data/raw/sinan-sifg/*.csv | wc -l
du -sh data/raw/* data/raw
```

> **Dica operacional:** o portal do SISAB sofre bastante oscilação durante o horário comercial (quando a página de ~6 MB pode levar mais de 30 s apenas para carregar). Para cargas históricas completas, prefira rodar em **rede estável, à noite ou em fins de semana**.

---

## 6. Exemplo Real de Execução (`sinan-sifg`)

Na execução completa da série histórica do módulo **`sinan-sifg`** (`CID O981`, de `2013-04` a `2026-10`, totalizando **163 competências**), o coletor concluiu **100% da janela sem erros finais**:

| Métrica | Resultado observado |
| :--- | :--- |
| **Competências avaliadas** | 163 (`2013-04` a `2026-10`) |
| **Meses com dados (`downloaded`)** | 150 (`2014-05` a `2026-10`) |
| **Meses sem registros (`no_data`)** | 13 (`2013-04` a `2014-04`) |
| **Erros pendentes ao final** | 0 *(1 falha pontual em `2017-09` recuperada automaticamente na 2ª tentativa)* |
| **Linhas municipais extraídas** | 34.236 registros |
| **Espaço ocupado em disco** | **1,2 MB** (150 arquivos CSV brutos) |
| **Tempo ativo somado das consultas** | **~22 minutos** (~1.318 s) |
| **Tempo total de relógio** | **~24 a 25 minutos** *(incluindo pausas de 2,5–5 s, 3 renovações preventivas de sessão e 1 retentativa)* |

### Por que meses `no_data` demoram bem mais que meses com dados?

Esse comportamento é esperado e vem da arquitetura do próprio portal SISAB:

* **Quando há dados (`downloaded`) — média de `~5,36 s` por competência:** o servidor devolve diretamente o fluxo `text/csv` (HTTP `200`) na sessão já aberta, sem recarregar a página. No lote do `sinan-sifg`, os 150 meses com dados consumiram **~13 min 24 s** somados (com a maioria das competências fechando entre `3,2 s` e `6,0 s`).
* **Quando não há dados (`no_data`) — média de `~39,5 s` por competência:** além da resposta HTTP `204`, o *postback* JSF do SISAB força o recarregamento completo da página (~6 MB de HTML), e o coletor precisa aguardar o DOM estabilizar e verificar se os filtros sobreviveram (recriando a sessão se o formulário tiver sido limpo). Apenas os 13 meses iniciais sem dados (`2013-04` a `2014-04`, com pico de `52,53 s` em `2013-10`) consumiram **~8 min 34 s** — quase 40% do tempo ativo de todo o módulo.

Portanto, ao rodar agravos mais raros (como tétano neonatal ou rubéola congênita), que podem possuir mais meses sem registros no início da série histórica, é normal que o tempo total seja maior mesmo gerando menos arquivos em disco.

### Trecho real do log da execução

```text
[sinan-sifg] iniciando extracao de 163 competencia(s) (total na janela: 163).
[sinan-sifg] (1/163) 2013-04: no_data em 31.08s
[sinan-sifg] (2/163) 2013-05: no_data em 33.55s
...
[sinan-sifg] (13/163) 2014-04: no_data em 48.79s
[sinan-sifg] (14/163) 2014-05: downloaded (1 registros) em 2.86s
[sinan-sifg] (15/163) 2014-06: downloaded (1 registros) em 3.48s
...
[sinan-sifg] renovando sessao jsf preventiva...
[sinan-sifg] 2017-09 falhou na tentativa 1/3: ErroServidorSisab: formulario principal indisponivel (...)
[sinan-sifg] aguardando 15s antes de tentar novamente...
[sinan-sifg] (54/163) 2017-09: downloaded (105 registros) em 3.85s
...
[sinan-sifg] (162/163) 2026-09: downloaded (588 registros) em 4.14s
[sinan-sifg] (163/163) 2026-10: downloaded (562 registros) em 1.42s
resumo do checkpoint:
  sinan-sifg   | downloaded | 150
  sinan-sifg   | no_data    | 13
```