from __future__ import annotations

import argparse
import csv
import hashlib
import json
import platform
import re
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"
REGISTRO_PATH = BASE_DIR / "registro" / "registro_bronze.csv"
MUNICIPIOS_REF_PATH = BASE_DIR / "referencias" / "municipios_ibge.csv"

SILVER_CSV_PATH = SILVER_DIR / "silver_sisab_valor.csv"
GOLD_MUN_CSV_PATH = GOLD_DIR / "gold_sisab_municipio_ano.csv"
GOLD_UF_CSV_PATH = GOLD_DIR / "gold_sisab_uf_ano.csv"

RELATORIO_E1_PATH = BASE_DIR / "registro" / "relatorio_e1_bronze.json"
RELATORIO_E2_PATH = SILVER_DIR / "relatorio_e2_silver.json"
RELATORIO_E3_PATH = GOLD_DIR / "relatorio_e3_gold.json"
RELATORIO_E4_PATH = GOLD_DIR / "relatorio_e4_validacao.json"

MAPA_COBERTURA_CSV_PATH = GOLD_DIR / "mapa_cobertura_uf_ano.csv"
MAPA_COBERTURA_MD_PATH = GOLD_DIR / "mapa_cobertura_sisab.md"

VERSAO_PIPELINE = "sisab-1.0.0"
SISTEMA_FONTE = "sisab"
PACOTES = [
    "pandas",
    "numpy",
    "pyarrow",
    "scipy",
    "splink",
    "duckdb",
    "pysus",
    "playwright",
]

PADRAO_ARQUIVO_LOTE = re.compile(
    r"^SISAB_(?P<tema>[a-z0-9_-]+)_(?P<competencia>\d{6})_(?P<id_consulta>[LJ]\d{4})\.csv$"
)


def formatar_caminho(caminho: Path, base: Path) -> str:
    # exibe caminho relativo a raiz do projeto quando possivel
    try:
        return caminho.resolve().relative_to(base.resolve()).as_posix()
    except ValueError:
        return caminho.as_posix()


def sha256_arquivo(caminho: Path) -> str:
    # calcula hash sha-256 em blocos para rastreabilidade das saidas
    hasher = hashlib.sha256()
    with caminho.open("rb") as arquivo:
        for bloco in iter(lambda: arquivo.read(65536), b""):
            hasher.update(bloco)
    return hasher.hexdigest()


def _versoes() -> dict[str, str | None]:
    # coleta versao do python, sistema operacional e pacotes relevantes
    v: dict[str, str | None] = {
        "python": sys.version.split()[0],
        "sistema": platform.platform(),
    }
    for pacote in PACOTES:
        try:
            v[pacote] = metadata.version(pacote)
        except metadata.PackageNotFoundError:
            v[pacote] = None
    return v


def _git(raiz: Path) -> str | None:
    # captura o hash curto do commit git atual se disponivel
    try:
        return (
            subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=raiz,
                capture_output=True,
                text=True,
                timeout=5,
            ).stdout.strip()
            or None
        )
    except Exception:
        return None


def _entradas(bronze: Path) -> list[dict[str, object]]:
    # resume os arquivos brutos do sisab por tema e ano com contagem, tamanho e datas de modificacao
    grupos: dict[tuple[str, str], list[Path]] = defaultdict(list)
    if bronze.exists():
        for caminho in sorted(bronze.rglob("*.csv")):
            match = PADRAO_ARQUIVO_LOTE.match(caminho.name)
            if match:
                tema = match.group("tema")
                ano = match.group("competencia")[:4]
            else:
                tema = caminho.parent.name
                ano = "indefinido"
            grupos[(tema, ano)].append(caminho)

    linhas: list[dict[str, object]] = []
    for (tema, ano), arqs in sorted(grupos.items()):
        tempos = [a.stat().st_mtime for a in arqs]
        linhas.append(
            {
                "sistema": SISTEMA_FONTE,
                "tema": tema,
                "ano": ano,
                "arquivos": len(arqs),
                "bytes": sum(a.stat().st_size for a in arqs),
                "modificado_de": datetime.fromtimestamp(
                    min(tempos), timezone.utc
                ).isoformat(timespec="seconds"),
                "modificado_ate": datetime.fromtimestamp(
                    max(tempos), timezone.utc
                ).isoformat(timespec="seconds"),
            }
        )
    return linhas


def gravar(
    gold: Path,
    bronze: Path,
    raiz: Path,
    execucao: dict[str, object],
    resultados: list[dict[str, object]],
    falhas: list[dict[str, object] | str],
) -> Path:
    # grava o manifesto json da execucao no padrao do pipeline sofia
    agora = datetime.now(timezone.utc)
    parametros_padrao: dict[str, object] = {
        "VERSAO_PIPELINE": VERSAO_PIPELINE,
        "SISTEMA_FONTE": SISTEMA_FONTE,
        "BRONZE_DIR": formatar_caminho(bronze, raiz),
        "GOLD_DIR": formatar_caminho(gold, raiz),
    }
    parametros_usados = dict(parametros_padrao)
    parametros_exec = execucao.get("parametros")
    if isinstance(parametros_exec, dict):
        parametros_usados.update(parametros_exec)

    dados = {
        "gerado_em_utc": agora.isoformat(timespec="seconds"),
        "versao_pipeline": VERSAO_PIPELINE,
        "commit_git": _git(raiz),
        "execucao": {k: v for k, v in execucao.items() if k != "parametros"},
        "parametros": parametros_usados,
        "versoes": _versoes(),
        "arquivos_de_entrada": _entradas(bronze),
        "resultados": resultados,
        "falhas": falhas,
    }
    pasta = gold / "manifestos"
    pasta.mkdir(parents=True, exist_ok=True)
    destino = pasta / f"manifesto_{agora.strftime('%Y%m%d_%H%M%S')}.json"
    destino.write_text(
        json.dumps(dados, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return destino


def carregar_json_opcional(caminho: Path) -> dict[str, object] | None:
    # le relatorio json de etapa anterior caso exista em disco
    if not caminho.exists():
        return None
    return json.loads(caminho.read_text(encoding="utf-8"))


def carregar_registro(caminho: Path) -> list[dict[str, str]]:
    # carrega o registro da bronze como lista ordenada de lotes
    if not caminho.exists():
        return []
    with caminho.open("r", encoding="utf-8-sig", newline="") as arquivo:
        return [
            {k: (v or "").strip() for k, v in linha.items()}
            for linha in csv.DictReader(arquivo)
        ]


def carregar_contagem_municipios_por_uf(caminho: Path) -> dict[str, int]:
    # conta o total oficial de municipios por uf na tabela de referencia do ibge
    contagem: dict[str, int] = defaultdict(int)
    if not caminho.exists():
        return {}
    with caminho.open("r", encoding="utf-8-sig", newline="") as arquivo:
        for linha in csv.DictReader(arquivo):
            uf = (linha.get("uf") or "").strip().upper()
            if uf:
                contagem[uf] += 1
    return dict(contagem)


def gerar_meses_esperados_por_ano(inicio: str, fim: str) -> dict[int, int]:
    # calcula quantas competencias mensais existem em cada ano da janela
    ano_ini, mes_ini = map(int, inicio.split("-"))
    ano_fim, mes_fim = map(int, fim.split("-"))
    por_ano: dict[int, int] = defaultdict(int)
    ano, mes = ano_ini, mes_ini
    while (ano, mes) <= (ano_fim, mes_fim):
        por_ano[ano] += 1
        mes += 1
        if mes > 12:
            mes = 1
            ano += 1
    return dict(por_ano)


def parsear_numero(texto: str) -> int | float:
    # converte valor textual da silver preservando inteiros quando possivel
    numero = float(texto.strip())
    if numero.is_integer():
        return int(numero)
    return numero


def gerar_mapa_cobertura(
    silver_csv_path: Path,
    registro_path: Path,
    municipios_ref_path: Path,
    relatorio_e4: dict[str, object] | None,
    saida_csv_path: Path,
    saida_md_path: Path,
) -> dict[str, object]:
    # gera mapa de cobertura em csv (tema x uf x ano) e resumo legivel em markdown
    if not silver_csv_path.exists():
        raise FileNotFoundError(f"arquivo silver nao encontrado: {silver_csv_path}")

    registros = carregar_registro(registro_path)
    muns_ibge_por_uf = carregar_contagem_municipios_por_uf(municipios_ref_path)
    total_muns_ibge = sum(muns_ibge_por_uf.values())

    janela_meses_por_tema: dict[str, dict[int, int]] = {}
    anos_janela: set[int] = set()
    temas_registrados: list[str] = []
    for reg in registros:
        tema = reg.get("tema", "")
        if not tema:
            continue
        temas_registrados.append(tema)
        meses_ano = gerar_meses_esperados_por_ano(
            reg["competencia_inicio"], reg["competencia_fim"]
        )
        janela_meses_por_tema[tema] = meses_ano
        anos_janela.update(meses_ano.keys())

    meses_tema_uf_ano: dict[tuple[str, str, int], set[int]] = defaultdict(set)
    muns_tema_uf_ano: dict[tuple[str, str, int], set[str]] = defaultdict(set)
    atend_tema_uf_ano: dict[tuple[str, str, int], int | float] = defaultdict(int)

    meses_tema_ano: dict[tuple[str, int], set[int]] = defaultdict(set)
    muns_tema_ano: dict[tuple[str, int], set[str]] = defaultdict(set)
    atend_tema_ano: dict[tuple[str, int], int | float] = defaultdict(int)

    muns_uf_tema: dict[tuple[str, str], set[str]] = defaultdict(set)
    atend_uf_tema: dict[tuple[str, str], int | float] = defaultdict(int)

    temas_silver: set[str] = set()
    ufs_silver: set[str] = set()

    with silver_csv_path.open("r", encoding="utf-8-sig", newline="") as arquivo:
        for row in csv.DictReader(arquivo):
            tema = row["tema"].strip()
            uf = row["uf"].strip().upper()
            mun = row["mun"].strip()
            ano = int(row["ano"])
            mes = int(row["mes"])
            valor = parsear_numero(row["valor"])

            temas_silver.add(tema)
            ufs_silver.add(uf)
            anos_janela.add(ano)

            meses_tema_uf_ano[(tema, uf, ano)].add(mes)
            muns_tema_uf_ano[(tema, uf, ano)].add(mun)
            atend_tema_uf_ano[(tema, uf, ano)] += valor

            meses_tema_ano[(tema, ano)].add(mes)
            muns_tema_ano[(tema, ano)].add(mun)
            atend_tema_ano[(tema, ano)] += valor

            muns_uf_tema[(uf, tema)].add(mun)
            atend_uf_tema[(uf, tema)] += valor

    temas_ordenados = sorted(set(temas_registrados) | temas_silver)
    ufs_ordenadas = sorted(set(muns_ibge_por_uf.keys()) | ufs_silver)
    anos_ordenados = sorted(anos_janela)

    # grava tabela csv detalhada de cobertura por tema, uf e ano
    linhas_csv: list[dict[str, object]] = []
    for tema in temas_ordenados:
        meses_esp_tema = janela_meses_por_tema.get(tema, {})
        for ano in anos_ordenados:
            meses_esp = meses_esp_tema.get(ano, 12)
            for uf in ufs_ordenadas:
                meses_com = len(meses_tema_uf_ano.get((tema, uf, ano), set()))
                muns_com = len(muns_tema_uf_ano.get((tema, uf, ano), set()))
                muns_uf_ref = muns_ibge_por_uf.get(uf, 0)
                pct_muns = (muns_com / muns_uf_ref * 100.0) if muns_uf_ref else 0.0
                atend = atend_tema_uf_ano.get((tema, uf, ano), 0)
                if isinstance(atend, float) and atend.is_integer():
                    atend = int(atend)

                linhas_csv.append(
                    {
                        "tema": tema,
                        "uf": uf,
                        "ano": ano,
                        "meses_esperados_ano": meses_esp,
                        "meses_com_valor": meses_com,
                        "municipios_uf_ibge": muns_uf_ref,
                        "municipios_com_valor": muns_com,
                        "pct_municipios_uf": round(pct_muns, 2),
                        "atendimentos": atend,
                    }
                )

    saida_csv_path.parent.mkdir(parents=True, exist_ok=True)
    colunas_csv = [
        "tema",
        "uf",
        "ano",
        "meses_esperados_ano",
        "meses_com_valor",
        "municipios_uf_ibge",
        "municipios_com_valor",
        "pct_municipios_uf",
        "atendimentos",
    ]
    with saida_csv_path.open("w", encoding="utf-8", newline="") as arquivo:
        escritor = csv.DictWriter(arquivo, fieldnames=colunas_csv)
        escritor.writeheader()
        escritor.writerows(linhas_csv)

    # monta resumo visual de cobertura em markdown
    linhas_md: list[str] = [
        "# Mapa de Cobertura — Pipeline SISAB",
        "",
        f"- gerado em (utc): `{datetime.now(timezone.utc).isoformat(timespec='seconds')}`",
        f"- versao do pipeline: `{VERSAO_PIPELINE}`",
        f"- temas cobertos: `{', '.join(temas_ordenados)}`",
        f"- anos na janela: `{min(anos_ordenados)} a {max(anos_ordenados)}` ({len(anos_ordenados)} anos)",
        f"- municipios de referencia ibge: `{total_muns_ibge}` ({len(ufs_ordenadas)} UFs)",
        "",
    ]

    # inclui tabela consolidada da v5 se o relatorio e4 estiver disponivel
    if relatorio_e4:
        v5 = relatorio_e4.get("validacoes", {}).get("V5_cobertura", {})
        por_tema = v5.get("por_tema", []) if isinstance(v5, dict) else []
        if por_tema:
            linhas_md.extend(
                [
                    "## Resumo Geral de Cobertura por Tema (V5)",
                    "",
                    "| tema | competencias com valor | municipios com valor | cobertura municipal (%) | celulas sem valor (%) | primeira comp | ultima comp |",
                    "| :--- | :---: | :---: | :---: | :---: | :---: | :---: |",
                ]
            )
            for item in por_tema:
                linhas_md.append(
                    f"| `{item['tema']}` | "
                    f"{item['competencias_com_valor']}/{item['competencias_janela']} | "
                    f"{item['municipios_com_valor']}/{item['municipios_referencia']} | "
                    f"{item['pct_municipios_com_valor']:.1f}% | "
                    f"{item['pct_celulas_sem_valor']:.2f}% | "
                    f"{item['primeira_competencia_com_valor'] or '-'} | "
                    f"{item['ultima_competencia_com_valor'] or '-'} |"
                )
            linhas_md.append("")

    # matriz tema x ano de meses com registro
    linhas_md.extend(
        [
            "## Cobertura Temporal (Meses com Valor / Meses na Janela por Ano)",
            "",
            "| ano | " + " | ".join(f"`{t}`" for t in temas_ordenados) + " |",
            "| :---: | " + " | ".join(":---:" for _ in temas_ordenados) + " |",
        ]
    )
    for ano in anos_ordenados:
        celulas = []
        for tema in temas_ordenados:
            meses_esp = janela_meses_por_tema.get(tema, {}).get(ano, 12)
            meses_com = len(meses_tema_ano.get((tema, ano), set()))
            celulas.append(f"{meses_com}/{meses_esp}")
        linhas_md.append(f"| {ano} | " + " | ".join(celulas) + " |")
    linhas_md.append("")

    # matriz uf x tema de cobertura municipal acumulada na serie
    linhas_md.extend(
        [
            "## Cobertura Territorial por UF (Municipios com Valor / Total IBGE)",
            "",
            "| UF | muns IBGE | " + " | ".join(f"`{t}`" for t in temas_ordenados) + " |",
            "| :---: | :---: | " + " | ".join(":---:" for _ in temas_ordenados) + " |",
        ]
    )
    for uf in ufs_ordenadas:
        tot_uf = muns_ibge_por_uf.get(uf, 0)
        celulas = []
        for tema in temas_ordenados:
            muns_com = len(muns_uf_tema.get((uf, tema), set()))
            pct = (muns_com / tot_uf * 100.0) if tot_uf else 0.0
            celulas.append(f"{muns_com} ({pct:.0f}%)")
        linhas_md.append(f"| {uf} | {tot_uf} | " + " | ".join(celulas) + " |")
    linhas_md.append("")

    saida_md_path.write_text("\n".join(linhas_md), encoding="utf-8")

    return {
        "mapa_csv": formatar_caminho(saida_csv_path, BASE_DIR),
        "mapa_md": formatar_caminho(saida_md_path, BASE_DIR),
        "n_linhas_csv": len(linhas_csv),
        "sha256_csv": sha256_arquivo(saida_csv_path),
        "sha256_md": sha256_arquivo(saida_md_path),
    }


def coletar_resultados_e_falhas(
    relatorio_e1: dict[str, object] | None,
    relatorio_e2: dict[str, object] | None,
    relatorio_e3: dict[str, object] | None,
    relatorio_e4: dict[str, object] | None,
    resumo_cobertura: dict[str, object],
    silver_csv_path: Path,
    gold_mun_path: Path,
    gold_uf_path: Path,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    # consolida os resultados e eventuais falhas de todas as etapas do pipeline
    resultados: list[dict[str, object]] = []
    falhas: list[dict[str, object]] = []

    if relatorio_e1 is not None:
        totais = relatorio_e1.get("totais", {})
        resultados.append(
            {
                "etapa": "E1_conferir_bronze",
                "status": relatorio_e1.get("status"),
                "gerado_em_utc": relatorio_e1.get("gerado_em_utc"),
                "lotes_registrados": totais.get("lotes_registrados"),
                "arquivos_bronze": totais.get("arquivos_bronze"),
                "linhas_dados_somadas": totais.get("linhas_dados_somadas"),
            }
        )
        for erro in relatorio_e1.get("erros", []):
            falhas.append({"etapa": "E1_conferir_bronze", "erro": erro})
    else:
        falhas.append({"etapa": "E1_conferir_bronze", "erro": "relatorio e1 ausente"})

    if relatorio_e2 is not None:
        totais = relatorio_e2.get("totais", {})
        resultados.append(
            {
                "etapa": "E2_padronizar_silver",
                "status": relatorio_e2.get("status"),
                "gerado_em_utc": relatorio_e2.get("gerado_em_utc"),
                "arquivo": formatar_caminho(silver_csv_path, BASE_DIR),
                "sha256": sha256_arquivo(silver_csv_path) if silver_csv_path.exists() else None,
                "linhas_silver_geradas": totais.get("linhas_silver_geradas"),
                "valores_invalidos": totais.get("valores_invalidos"),
            }
        )
        for erro in relatorio_e2.get("erros", []):
            falhas.append({"etapa": "E2_padronizar_silver", "erro": erro})
    else:
        falhas.append({"etapa": "E2_padronizar_silver", "erro": "relatorio e2 ausente"})

    if relatorio_e3 is not None:
        saidas = relatorio_e3.get("saidas", {})
        resultados.append(
            {
                "etapa": "E3_agregar_gold",
                "status": relatorio_e3.get("status"),
                "gerado_em_utc": relatorio_e3.get("gerado_em_utc"),
                "gold_municipio_ano": saidas.get("gold_municipio_ano"),
                "gold_uf_ano": saidas.get("gold_uf_ano"),
                "cobertura": relatorio_e3.get("cobertura"),
            }
        )
        for erro in relatorio_e3.get("erros", []):
            falhas.append({"etapa": "E3_agregar_gold", "erro": erro})
    else:
        falhas.append({"etapa": "E3_agregar_gold", "erro": "relatorio e3 ausente"})

    if relatorio_e4 is not None:
        vals = relatorio_e4.get("validacoes", {})
        resultados.append(
            {
                "etapa": "E4_validar",
                "status": relatorio_e4.get("status"),
                "gerado_em_utc": relatorio_e4.get("gerado_em_utc"),
                "resumo_validacoes": {
                    nome: bloco.get("status")
                    for nome, bloco in vals.items()
                    if isinstance(bloco, dict)
                },
            }
        )
        if relatorio_e4.get("status") != "aprovado":
            falhas.append(
                {
                    "etapa": "E4_validar",
                    "erro": "uma ou mais validacoes obrigatorias reprovadas",
                }
            )
    else:
        falhas.append({"etapa": "E4_validar", "erro": "relatorio e4 ausente"})

    resultados.append(
        {
            "etapa": "E5_relatar",
            "status": "aprovado",
            "mapa_cobertura_csv": resumo_cobertura["mapa_csv"],
            "mapa_cobertura_md": resumo_cobertura["mapa_md"],
            "n_linhas_mapa_csv": resumo_cobertura["n_linhas_csv"],
            "sha256_mapa_csv": resumo_cobertura["sha256_csv"],
            "sha256_mapa_md": resumo_cobertura["sha256_md"],
            "artefatos_gold_verificados": {
                "gold_municipio_ano_existe": gold_mun_path.exists(),
                "gold_uf_ano_existe": gold_uf_path.exists(),
            },
        }
    )

    return resultados, falhas


def executar_relatorio_e5(
    bronze_dir: Path = BRONZE_DIR,
    gold_dir: Path = GOLD_DIR,
    registro_path: Path = REGISTRO_PATH,
    municipios_ref_path: Path = MUNICIPIOS_REF_PATH,
    silver_csv_path: Path = SILVER_CSV_PATH,
    gold_mun_path: Path = GOLD_MUN_CSV_PATH,
    gold_uf_path: Path = GOLD_UF_CSV_PATH,
    relatorio_e1_path: Path = RELATORIO_E1_PATH,
    relatorio_e2_path: Path = RELATORIO_E2_PATH,
    relatorio_e3_path: Path = RELATORIO_E3_PATH,
    relatorio_e4_path: Path = RELATORIO_E4_PATH,
    saida_mapa_csv: Path = MAPA_COBERTURA_CSV_PATH,
    saida_mapa_md: Path = MAPA_COBERTURA_MD_PATH,
    execucao_extra: dict[str, object] | None = None,
) -> int:
    # gera mapa de cobertura e grava manifesto consolidado da execucao do pipeline
    relatorio_e1 = carregar_json_opcional(relatorio_e1_path)
    relatorio_e2 = carregar_json_opcional(relatorio_e2_path)
    relatorio_e3 = carregar_json_opcional(relatorio_e3_path)
    relatorio_e4 = carregar_json_opcional(relatorio_e4_path)

    try:
        resumo_cobertura = gerar_mapa_cobertura(
            silver_csv_path=silver_csv_path,
            registro_path=registro_path,
            municipios_ref_path=municipios_ref_path,
            relatorio_e4=relatorio_e4,
            saida_csv_path=saida_mapa_csv,
            saida_md_path=saida_mapa_md,
        )
    except FileNotFoundError as exc:
        print(f"[erro] {exc}")
        return 1

    resultados, falhas = coletar_resultados_e_falhas(
        relatorio_e1=relatorio_e1,
        relatorio_e2=relatorio_e2,
        relatorio_e3=relatorio_e3,
        relatorio_e4=relatorio_e4,
        resumo_cobertura=resumo_cobertura,
        silver_csv_path=silver_csv_path,
        gold_mun_path=gold_mun_path,
        gold_uf_path=gold_uf_path,
    )

    execucao: dict[str, object] = {
        "modo": "e5_standalone",
        "status_geral": "aprovado" if not falhas else "com_falhas",
        "etapas_consolidacao": ["E1", "E2", "E3", "E4", "E5"],
        "parametros": {
            "REGISTRO_PATH": formatar_caminho(registro_path, BASE_DIR),
            "MUNICIPIOS_REF_PATH": formatar_caminho(municipios_ref_path, BASE_DIR),
            "SILVER_CSV_PATH": formatar_caminho(silver_csv_path, BASE_DIR),
            "GOLD_MUN_CSV_PATH": formatar_caminho(gold_mun_path, BASE_DIR),
            "GOLD_UF_CSV_PATH": formatar_caminho(gold_uf_path, BASE_DIR),
        },
    }
    if execucao_extra:
        params_extra = execucao_extra.get("parametros")
        for chave, valor in execucao_extra.items():
            if chave != "parametros":
                execucao[chave] = valor
        if isinstance(params_extra, dict) and isinstance(execucao["parametros"], dict):
            execucao["parametros"].update(params_extra)

    manifesto_path = gravar(
        gold=gold_dir,
        bronze=bronze_dir,
        raiz=BASE_DIR,
        execucao=execucao,
        resultados=resultados,
        falhas=falhas,
    )

    print("relatorio e5 (manifesto e mapa de cobertura):")
    print(f"  etapas consolidadas : {len(resultados)}")
    print(f"  falhas registradas  : {len(falhas)}")
    print(f"  linhas mapa csv     : {resumo_cobertura['n_linhas_csv']}")
    print(f"  mapa cobertura csv  : {resumo_cobertura['mapa_csv']}")
    print(f"  mapa cobertura md   : {resumo_cobertura['mapa_md']}")
    print(f"  manifesto salvo em  : {formatar_caminho(manifesto_path, BASE_DIR)}")

    if falhas:
        print("\nfalhas registradas no manifesto:")
        for falha in falhas:
            print(f"  [falha] {falha}")
        print("status e5: concluido com falhas")
        return 1

    print("status e5: aprovado")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="etapa e5: gera manifesto da execucao e mapa de cobertura do pipeline sisab."
    )
    parser.add_argument("--bronze-dir", type=Path, default=BRONZE_DIR)
    parser.add_argument("--gold-dir", type=Path, default=GOLD_DIR)
    parser.add_argument("--registro", type=Path, default=REGISTRO_PATH)
    parser.add_argument("--municipios-ref", type=Path, default=MUNICIPIOS_REF_PATH)
    parser.add_argument("--silver-csv", type=Path, default=SILVER_CSV_PATH)
    parser.add_argument("--gold-mun", type=Path, default=GOLD_MUN_CSV_PATH)
    parser.add_argument("--gold-uf", type=Path, default=GOLD_UF_CSV_PATH)
    parser.add_argument("--relatorio-e1", type=Path, default=RELATORIO_E1_PATH)
    parser.add_argument("--relatorio-e2", type=Path, default=RELATORIO_E2_PATH)
    parser.add_argument("--relatorio-e3", type=Path, default=RELATORIO_E3_PATH)
    parser.add_argument("--relatorio-e4", type=Path, default=RELATORIO_E4_PATH)
    parser.add_argument("--saida-mapa-csv", type=Path, default=MAPA_COBERTURA_CSV_PATH)
    parser.add_argument("--saida-mapa-md", type=Path, default=MAPA_COBERTURA_MD_PATH)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return executar_relatorio_e5(
        bronze_dir=args.bronze_dir,
        gold_dir=args.gold_dir,
        registro_path=args.registro,
        municipios_ref_path=args.municipios_ref,
        silver_csv_path=args.silver_csv,
        gold_mun_path=args.gold_mun,
        gold_uf_path=args.gold_uf,
        relatorio_e1_path=args.relatorio_e1,
        relatorio_e2_path=args.relatorio_e2,
        relatorio_e3_path=args.relatorio_e3,
        relatorio_e4_path=args.relatorio_e4,
        saida_mapa_csv=args.saida_mapa_csv,
        saida_mapa_md=args.saida_mapa_md,
    )


if __name__ == "__main__":
    sys.exit(main())