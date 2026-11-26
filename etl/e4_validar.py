from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
import sys
import unicodedata
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"
REGISTRO_PATH = BASE_DIR / "registro" / "registro_bronze.csv"
SILVER_CSV_PATH = BASE_DIR / "data" / "silver" / "silver_sisab_valor.csv"
GOLD_DIR = BASE_DIR / "data" / "gold"
GOLD_MUN_CSV_PATH = GOLD_DIR / "gold_sisab_municipio_ano.csv"
GOLD_UF_CSV_PATH = GOLD_DIR / "gold_sisab_uf_ano.csv"
MUNICIPIOS_REF_PATH = BASE_DIR / "referencias" / "municipios_ibge.csv"
RELATORIO_E4_PATH = GOLD_DIR / "relatorio_e4_validacao.json"
COBERTURA_V5_CSV_PATH = GOLD_DIR / "cobertura_v5_por_tema.csv"
COMPARACAO_V6_CSV_PATH = GOLD_DIR / "comparacao_v6_externa.csv"

URL_IBGE_MUNICIPIOS = (
    "https://servicodados.ibge.gov.br/api/v1/localidades/municipios?view=nivelado"
)
PADRAO_ID_SUFIXO = re.compile(r"_(?P<id_consulta>[LJ]\d{4})\.csv$")

UF_PARA_COD_IBGE = {
    "RO": "11",
    "AC": "12",
    "AM": "13",
    "RR": "14",
    "PA": "15",
    "AP": "16",
    "TO": "17",
    "MA": "21",
    "PI": "22",
    "CE": "23",
    "RN": "24",
    "PB": "25",
    "PE": "26",
    "AL": "27",
    "SE": "28",
    "BA": "29",
    "MG": "31",
    "ES": "32",
    "RJ": "33",
    "SP": "35",
    "PR": "41",
    "SC": "42",
    "RS": "43",
    "MS": "50",
    "MT": "51",
    "GO": "52",
    "DF": "53",
}


def normalizar_texto(texto: str) -> str:
    # remove acentos e espacos excedentes para comparacoes seguras
    sem_acento = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    return " ".join(sem_acento.strip().lower().split())


def formatar_caminho(caminho: Path, base: Path) -> str:
    # exibe caminho relativo a raiz do projeto quando possivel
    try:
        return caminho.resolve().relative_to(base.resolve()).as_posix()
    except ValueError:
        return caminho.as_posix()


def parsear_numero(texto: str) -> int | float:
    # converte texto numerico preservando inteiros quando possivel
    limpo = texto.strip()
    if "," in limpo:
        limpo = limpo.replace(".", "").replace(",", ".")
    elif re.fullmatch(r"\d{1,3}(?:\.\d{3})+", limpo):
        limpo = limpo.replace(".", "")
    numero = float(limpo)
    if numero.is_integer():
        return int(numero)
    return numero


def gerar_competencias(inicio: str, fim: str) -> list[str]:
    # expande intervalo aaaa-mm em lista ordenada de competencias mensais
    ano_ini, mes_ini = map(int, inicio.split("-"))
    ano_fim, mes_fim = map(int, fim.split("-"))
    competencias: list[str] = []
    ano, mes = ano_ini, mes_ini
    while (ano, mes) <= (ano_fim, mes_fim):
        competencias.append(f"{ano:04d}-{mes:02d}")
        mes += 1
        if mes > 12:
            mes = 1
            ano += 1
    return competencias


def baixar_tabela_municipios_ibge(destino: Path, timeout: int = 30) -> int:
    # consulta a api oficial de localidades do ibge e gera referencias/municipios_ibge.csv
    req = urllib.request.Request(
        URL_IBGE_MUNICIPIOS,
        headers={"User-Agent": "sisab-etl-validacao/1.0"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    if not isinstance(payload, list) or not payload:
        raise ValueError("resposta vazia ou inesperada da api de localidades do ibge")

    linhas: list[dict[str, str]] = []
    for item in payload:
        cod7 = str(item.get("municipio-id") or item.get("id") or "").strip()
        nome = str(item.get("municipio-nome") or item.get("nome") or "").strip()
        uf_sigla = str(item.get("UF-sigla") or "").strip().upper()
        uf_cod = str(item.get("UF-id") or "").strip()
        regiao = str(item.get("regiao-nome") or "").strip()

        # fallback caso a resposta venha no formato hierarquico padrao
        if not uf_sigla:
            reg_imed = item.get("regiao-imediata") or {}
            reg_inter = reg_imed.get("regiao-intermediaria") or {}
            uf_obj = reg_inter.get("UF") or {}
            if not uf_obj:
                micro = item.get("microrregiao") or {}
                meso = micro.get("mesorregiao") or {}
                uf_obj = meso.get("UF") or {}
            uf_sigla = str(uf_obj.get("sigla") or "").strip().upper()
            uf_cod = str(uf_obj.get("id") or "").strip()
            regiao = str((uf_obj.get("regiao") or {}).get("nome") or "").strip()

        if len(cod7) == 7 and cod7.isdigit():
            linhas.append(
                {
                    "cod_ibge_6": cod7[:6],
                    "cod_ibge_7": cod7,
                    "nome_municipio": nome,
                    "uf": uf_sigla,
                    "cod_uf": uf_cod or cod7[:2],
                    "regiao": regiao,
                }
            )

    linhas.sort(key=lambda r: r["cod_ibge_6"])
    destino.parent.mkdir(parents=True, exist_ok=True)
    with destino.open("w", encoding="utf-8", newline="") as arquivo:
        escritor = csv.DictWriter(
            arquivo,
            fieldnames=["cod_ibge_6", "cod_ibge_7", "nome_municipio", "uf", "cod_uf", "regiao"],
        )
        escritor.writeheader()
        escritor.writerows(linhas)

    return len(linhas)


def carregar_tabela_municipios(
    caminho: Path, baixar_se_ausente: bool = True, forcar_download: bool = False
) -> dict[str, dict[str, str]]:
    # carrega tabela de referencia de municipios indexada por codigo ibge de seis digitos
    if forcar_download or (not caminho.exists() and baixar_se_ausente):
        print(f"baixando tabela oficial de municipios do ibge em {URL_IBGE_MUNICIPIOS} ...")
        qtd = baixar_tabela_municipios_ibge(caminho)
        print(f"tabela salva em {formatar_caminho(caminho, BASE_DIR)} ({qtd} municipios)")

    if not caminho.exists():
        raise FileNotFoundError(
            f"tabela de municipios nao encontrada em {caminho}; execute com --baixar-municipios"
        )

    referencia: dict[str, dict[str, str]] = {}
    with caminho.open("r", encoding="utf-8-sig", newline="") as arquivo:
        leitor = csv.DictReader(arquivo)
        for linha in leitor:
            cod6 = (linha.get("cod_ibge_6") or "").strip()
            if not cod6:
                cod7 = (linha.get("cod_ibge_7") or "").strip()
                cod6 = cod7[:6] if len(cod7) >= 6 else ""
            if len(cod6) == 6 and cod6.isdigit():
                referencia[cod6] = {
                    "cod_ibge_6": cod6,
                    "cod_ibge_7": (linha.get("cod_ibge_7") or "").strip(),
                    "nome_municipio": (linha.get("nome_municipio") or "").strip(),
                    "uf": (linha.get("uf") or "").strip().upper(),
                    "cod_uf": (linha.get("cod_uf") or cod6[:2]).strip(),
                    "regiao": (linha.get("regiao") or "").strip(),
                }
    return referencia


def carregar_registro(caminho: Path) -> dict[str, dict[str, str]]:
    # carrega o registro da camada bronze indexado por id_consulta
    if not caminho.exists():
        raise FileNotFoundError(f"registro nao encontrado: {caminho}")
    registros: dict[str, dict[str, str]] = {}
    with caminho.open("r", encoding="utf-8-sig", newline="") as arquivo:
        for linha in csv.DictReader(arquivo):
            id_c = (linha.get("id_consulta") or "").strip()
            if id_c:
                registros[id_c] = {k: (v or "").strip() for k, v in linha.items()}
    return registros


def validar_v1_registro_completo(
    bronze_dir: Path, registros: dict[str, dict[str, str]]
) -> dict[str, object]:
    # verifica correspondencia de cem por cento entre arquivos da bronze e linhas do registro
    arquivos_csv = sorted(bronze_dir.rglob("*.csv")) if bronze_dir.exists() else []
    arquivos_por_id: dict[str, int] = defaultdict(int)
    arquivos_sem_id: list[str] = []
    arquivos_orfaos: list[str] = []

    for caminho in arquivos_csv:
        rel = formatar_caminho(caminho, BASE_DIR)
        match = PADRAO_ID_SUFIXO.search(caminho.name)
        if not match:
            arquivos_sem_id.append(rel)
            continue
        id_c = match.group("id_consulta")
        if id_c not in registros:
            arquivos_orfaos.append(rel)
            continue
        arquivos_por_id[id_c] += 1

    registros_sem_arquivo = sorted(
        id_c for id_c in registros if arquivos_por_id.get(id_c, 0) == 0
    )
    total_arquivos = len(arquivos_csv)
    arquivos_vinculados = total_arquivos - len(arquivos_sem_id) - len(arquivos_orfaos)
    pct_arquivos = (arquivos_vinculados / total_arquivos * 100.0) if total_arquivos else 0.0
    pct_registros = (
        (len(registros) - len(registros_sem_arquivo)) / len(registros) * 100.0
        if registros
        else 0.0
    )

    aprovado = (
        total_arquivos > 0
        and len(arquivos_sem_id) == 0
        and len(arquivos_orfaos) == 0
        and len(registros_sem_arquivo) == 0
    )
    return {
        "status": "aprovado" if aprovado else "reprovado",
        "arquivos_bronze_total": total_arquivos,
        "arquivos_vinculados": arquivos_vinculados,
        "pct_arquivos_no_registro": round(pct_arquivos, 4),
        "lotes_registrados_total": len(registros),
        "lotes_com_arquivo": len(registros) - len(registros_sem_arquivo),
        "pct_registros_com_arquivo": round(pct_registros, 4),
        "arquivos_sem_id": arquivos_sem_id,
        "arquivos_orfaos": arquivos_orfaos,
        "registros_sem_arquivo": registros_sem_arquivo,
    }


def somar_valores_bronze_por_tema(
    bronze_dir: Path, registros: dict[str, dict[str, str]]
) -> tuple[dict[str, int | float], list[str]]:
    # recalcula somas diretamente da bronze e checa linhas de total quando presentes no csv
    soma_bronze: dict[str, int | float] = defaultdict(int)
    divergencias_total_csv: list[str] = []

    for caminho in sorted(bronze_dir.rglob("*.csv")):
        match = PADRAO_ID_SUFIXO.search(caminho.name)
        if not match:
            continue
        id_c = match.group("id_consulta")
        if id_c not in registros:
            continue
        tema = registros[id_c]["tema"]

        with caminho.open("r", encoding="latin-1", errors="replace") as arquivo:
            linhas = [linha.rstrip("\r\n") for linha in arquivo]

        idx_cab = None
        for idx, linha in enumerate(linhas):
            if ";" not in linha:
                continue
            cols = [normalizar_texto(c) for c in linha.split(";")]
            if "ibge" in cols and ("municipio" in cols or "uf" in cols):
                idx_cab = idx
                break
        if idx_cab is None:
            continue

        cab = linhas[idx_cab].split(";")
        while cab and not cab[-1].strip():
            cab.pop()
        cols_norm = [normalizar_texto(c) for c in cab]
        idx_ibge = cols_norm.index("ibge")
        fixos = {
            i for i, c in enumerate(cols_norm) if c in {"uf", "ibge", "municipio"}
        }
        indices_medidas = [
            i for i, c in enumerate(cols_norm) if i not in fixos and c and c != "total"
        ]
        if not indices_medidas and "total" in cols_norm:
            indices_medidas = [cols_norm.index("total")]

        soma_arquivo = 0
        soma_linha_total = None
        for linha in linhas[idx_cab + 1 :]:
            stripada = linha.strip()
            if not stripada:
                continue
            norm = normalizar_texto(stripada)
            if norm.startswith("fonte:") or norm.startswith("dado gerado em:"):
                break
            partes = [p.strip() for p in stripada.split(";")]
            if not partes:
                continue
            if normalizar_texto(partes[0]).startswith("total"):
                total_parcial = 0
                for idx_col in indices_medidas:
                    if idx_col < len(partes) and partes[idx_col] not in {"", "-", "--"}:
                        total_parcial += parsear_numero(partes[idx_col])
                soma_linha_total = total_parcial
                continue
            if idx_ibge >= len(partes):
                continue
            ibge = partes[idx_ibge]
            if not (ibge.isdigit() and len(ibge) in (6, 7)):
                continue
            for idx_col in indices_medidas:
                if idx_col < len(partes) and partes[idx_col] not in {"", "-", "--"}:
                    soma_arquivo += parsear_numero(partes[idx_col])

        soma_bronze[tema] += soma_arquivo
        if soma_linha_total is not None and soma_linha_total != soma_arquivo:
            divergencias_total_csv.append(
                f"{caminho.name}: soma_municipios={soma_arquivo} != linha_total_csv={soma_linha_total}"
            )

    return soma_bronze, divergencias_total_csv


def validar_v2_soma_territorial(
    bronze_dir: Path,
    registros: dict[str, dict[str, str]],
    silver_rows: list[dict[str, str]],
    gold_mun_path: Path,
    gold_uf_path: Path,
) -> dict[str, object]:
    # verifica conservacao de somas entre bronze, silver, municipios, ufs e brasil
    soma_bronze_tema, div_total_csv = somar_valores_bronze_por_tema(bronze_dir, registros)

    soma_silver_tema: dict[str, int | float] = defaultdict(int)
    soma_silver_brasil_ano: dict[tuple[int, str], int | float] = defaultdict(int)
    for row in silver_rows:
        tema = row["tema"]
        ano = int(row["ano"])
        val = parsear_numero(row["valor"])
        soma_silver_tema[tema] += val
        soma_silver_brasil_ano[(ano, tema)] += val

    with gold_mun_path.open("r", encoding="utf-8-sig", newline="") as f_mun:
        leitor_mun = csv.DictReader(f_mun)
        colunas_mun = leitor_mun.fieldnames or []
        temas = [c for c in colunas_mun if c not in {"mun", "uf", "ano"}]
        soma_mun_por_uf_ano: dict[tuple[str, int, str], int | float] = defaultdict(int)
        for row in leitor_mun:
            uf = row["uf"].strip().upper()
            ano = int(row["ano"])
            for tema in temas:
                soma_mun_por_uf_ano[(uf, ano, tema)] += parsear_numero(row[tema])

    with gold_uf_path.open("r", encoding="utf-8-sig", newline="") as f_uf:
        leitor_uf = csv.DictReader(f_uf)
        soma_uf_direto: dict[tuple[str, int, str], int | float] = defaultdict(int)
        soma_uf_por_brasil_ano: dict[tuple[int, str], int | float] = defaultdict(int)
        for row in leitor_uf:
            uf = row["uf"].strip().upper()
            ano = int(row["ano"])
            for tema in temas:
                val = parsear_numero(row[tema])
                soma_uf_direto[(uf, ano, tema)] = val
                soma_uf_por_brasil_ano[(ano, tema)] += val

    div_bronze_silver: list[str] = []
    for tema in sorted(set(soma_bronze_tema) | set(soma_silver_tema)):
        if soma_bronze_tema.get(tema, 0) != soma_silver_tema.get(tema, 0):
            div_bronze_silver.append(
                f"{tema}: bronze={soma_bronze_tema.get(tema, 0)} != silver={soma_silver_tema.get(tema, 0)}"
            )

    div_mun_uf: list[str] = []
    for chave in sorted(set(soma_mun_por_uf_ano) | set(soma_uf_direto)):
        v_mun = soma_mun_por_uf_ano.get(chave, 0)
        v_uf = soma_uf_direto.get(chave, 0)
        if v_mun != v_uf:
            uf, ano, tema = chave
            div_mun_uf.append(f"{uf}/{ano}/{tema}: soma_muns={v_mun} != gold_uf={v_uf}")

    div_uf_brasil: list[str] = []
    for chave in sorted(set(soma_uf_por_brasil_ano) | set(soma_silver_brasil_ano)):
        v_uf = soma_uf_por_brasil_ano.get(chave, 0)
        v_br = soma_silver_brasil_ano.get(chave, 0)
        if v_uf != v_br:
            ano, tema = chave
            div_uf_brasil.append(f"BR/{ano}/{tema}: soma_ufs={v_uf} != brasil_silver={v_br}")

    aprovado = not (div_total_csv or div_bronze_silver or div_mun_uf or div_uf_brasil)
    return {
        "status": "aprovado" if aprovado else "reprovado",
        "totais_por_tema": {
            tema: {
                "bronze": soma_bronze_tema.get(tema, 0),
                "silver_brasil": soma_silver_tema.get(tema, 0),
                "gold_uf_somado": sum(
                    v for (a, t), v in soma_uf_por_brasil_ano.items() if t == tema
                ),
            }
            for tema in sorted(set(soma_silver_tema) | set(temas))
        },
        "divergencias_linha_total_bronze": div_total_csv,
        "divergencias_bronze_vs_silver": div_bronze_silver,
        "divergencias_municipios_vs_uf": div_mun_uf,
        "divergencias_uf_vs_brasil": div_uf_brasil,
    }


def validar_v4_chaves_municipios(
    silver_rows: list[dict[str, str]],
    gold_mun_path: Path,
    municipios_ref: dict[str, dict[str, str]],
) -> dict[str, object]:
    # confere se cem por cento dos codigos municipais de seis digitos existem na tabela do ibge
    chaves_silver: set[tuple[str, str]] = set()
    muns_silver: set[str] = set()
    linhas_silver_validas = 0

    for row in silver_rows:
        mun = row["mun"].strip()
        uf = row["uf"].strip().upper()
        chaves_silver.add((mun, uf))
        muns_silver.add(mun)
        if mun in municipios_ref and municipios_ref[mun]["uf"] == uf:
            linhas_silver_validas += 1

    muns_gold: set[str] = set()
    chaves_gold: set[tuple[str, str]] = set()
    linhas_gold_total = 0
    linhas_gold_validas = 0

    with gold_mun_path.open("r", encoding="utf-8-sig", newline="") as f_mun:
        for row in csv.DictReader(f_mun):
            linhas_gold_total += 1
            mun = row["mun"].strip()
            uf = row["uf"].strip().upper()
            muns_gold.add(mun)
            chaves_gold.add((mun, uf))
            if mun in municipios_ref and municipios_ref[mun]["uf"] == uf:
                linhas_gold_validas += 1

    todos_muns = muns_silver | muns_gold
    muns_invalidos = sorted(m for m in todos_muns if m not in municipios_ref)

    conflitos_uf: list[dict[str, str]] = []
    for mun, uf in sorted(chaves_silver | chaves_gold):
        if mun in municipios_ref:
            uf_oficial = municipios_ref[mun]["uf"]
            cod_uf_esperado = UF_PARA_COD_IBGE.get(uf, "")
            if uf != uf_oficial or (cod_uf_esperado and not mun.startswith(cod_uf_esperado)):
                conflitos_uf.append(
                    {
                        "mun": mun,
                        "uf_dado": uf,
                        "uf_ibge": uf_oficial,
                        "nome_ibge": municipios_ref[mun]["nome_municipio"],
                    }
                )

    muns_validos = len(todos_muns) - len(muns_invalidos)
    pct_muns_validos = (muns_validos / len(todos_muns) * 100.0) if todos_muns else 0.0
    pct_linhas_silver = (
        linhas_silver_validas / len(silver_rows) * 100.0 if silver_rows else 0.0
    )
    pct_linhas_gold = (
        linhas_gold_validas / linhas_gold_total * 100.0 if linhas_gold_total else 0.0
    )

    aprovado = len(muns_invalidos) == 0 and len(conflitos_uf) == 0 and len(todos_muns) > 0
    return {
        "status": "aprovado" if aprovado else "reprovado",
        "municipios_referencia_ibge": len(municipios_ref),
        "municipios_unicos_nos_dados": len(todos_muns),
        "municipios_validos": muns_validos,
        "pct_municipios_validos": round(pct_muns_validos, 4),
        "pct_linhas_silver_validas": round(pct_linhas_silver, 4),
        "pct_linhas_gold_validas": round(pct_linhas_gold, 4),
        "excecoes_municipios_inexistentes": muns_invalidos,
        "excecoes_conflito_uf": conflitos_uf,
    }


def validar_v5_cobertura(
    registros: dict[str, dict[str, str]],
    silver_rows: list[dict[str, str]],
    total_municipios_ref: int,
    saida_csv_path: Path,
) -> dict[str, object]:
    # calcula cobertura de municipios e competencias com e sem valor por tema
    janela_por_tema: dict[str, list[str]] = {}
    for reg in registros.values():
        tema = reg["tema"]
        janela_por_tema[tema] = gerar_competencias(
            reg["competencia_inicio"], reg["competencia_fim"]
        )

    comps_com_valor: dict[str, set[str]] = defaultdict(set)
    muns_com_valor: dict[str, set[str]] = defaultdict(set)
    celulas_com_valor: dict[str, set[tuple[str, str]]] = defaultdict(set)

    for row in silver_rows:
        tema = row["tema"]
        mun = row["mun"].strip()
        comp = f"{int(row['ano']):04d}-{int(row['mes']):02d}"
        comps_com_valor[tema].add(comp)
        muns_com_valor[tema].add(mun)
        celulas_com_valor[tema].add((mun, comp))

    resumo_temas: list[dict[str, object]] = []
    for tema in sorted(set(janela_por_tema) | set(comps_com_valor)):
        janela = janela_por_tema.get(tema, sorted(comps_com_valor[tema]))
        n_comps_janela = len(janela)
        n_comps_com = len(comps_com_valor[tema])
        n_comps_sem = max(n_comps_janela - n_comps_com, 0)
        comps_sem_lista = sorted(set(janela) - comps_com_valor[tema])

        n_muns_com = len(muns_com_valor[tema])
        n_muns_sem = max(total_municipios_ref - n_muns_com, 0)

        celulas_possiveis = total_municipios_ref * n_comps_janela
        n_celulas_com = len(celulas_com_valor[tema])
        n_celulas_sem = max(celulas_possiveis - n_celulas_com, 0)
        pct_celulas_com = (
            n_celulas_com / celulas_possiveis * 100.0 if celulas_possiveis else 0.0
        )
        pct_celulas_sem = (
            n_celulas_sem / celulas_possiveis * 100.0 if celulas_possiveis else 0.0
        )

        resumo_temas.append(
            {
                "tema": tema,
                "competencias_janela": n_comps_janela,
                "competencias_com_valor": n_comps_com,
                "competencias_sem_valor": n_comps_sem,
                "pct_competencias_com_valor": round(
                    (n_comps_com / n_comps_janela * 100.0) if n_comps_janela else 0.0, 2
                ),
                "municipios_referencia": total_municipios_ref,
                "municipios_com_valor": n_muns_com,
                "municipios_sem_valor": n_muns_sem,
                "pct_municipios_com_valor": round(
                    (n_muns_com / total_municipios_ref * 100.0) if total_municipios_ref else 0.0,
                    2,
                ),
                "celulas_mun_comp_possiveis": celulas_possiveis,
                "celulas_mun_comp_com_valor": n_celulas_com,
                "celulas_mun_comp_sem_valor": n_celulas_sem,
                "pct_celulas_com_valor": round(pct_celulas_com, 4),
                "pct_celulas_sem_valor": round(pct_celulas_sem, 4),
                "primeira_competencia_com_valor": min(comps_com_valor[tema])
                if comps_com_valor[tema]
                else None,
                "ultima_competencia_com_valor": max(comps_com_valor[tema])
                if comps_com_valor[tema]
                else None,
                "competencias_sem_valor_amostra": comps_sem_lista[:15],
            }
        )

    saida_csv_path.parent.mkdir(parents=True, exist_ok=True)
    colunas_csv = [
        "tema",
        "competencias_janela",
        "competencias_com_valor",
        "competencias_sem_valor",
        "pct_competencias_com_valor",
        "municipios_referencia",
        "municipios_com_valor",
        "municipios_sem_valor",
        "pct_municipios_com_valor",
        "celulas_mun_comp_possiveis",
        "celulas_mun_comp_com_valor",
        "celulas_mun_comp_sem_valor",
        "pct_celulas_com_valor",
        "pct_celulas_sem_valor",
        "primeira_competencia_com_valor",
        "ultima_competencia_com_valor",
    ]
    with saida_csv_path.open("w", encoding="utf-8", newline="") as arquivo:
        escritor = csv.DictWriter(arquivo, fieldnames=colunas_csv)
        escritor.writeheader()
        for item in resumo_temas:
            escritor.writerow({col: item[col] for col in colunas_csv})

    return {
        "status": "informado",
        "arquivo_cobertura_csv": formatar_caminho(saida_csv_path, BASE_DIR),
        "por_tema": resumo_temas,
    }


def validar_v6_base_externa(
    gold_uf_path: Path,
    base_externa_path: Path | None,
    tema_sisab: str | None,
    col_uf: str,
    col_ano: str,
    col_valor: str,
    saida_csv_path: Path,
) -> dict[str, object]:
    # compara gold_sisab_uf_ano com base externa opcional por uf e ano
    if base_externa_path is None:
        return {
            "status": "nao_executado",
            "motivo": "flag opcional --base-externa nao informada",
        }

    if not base_externa_path.exists():
        return {
            "status": "reprovado",
            "erro": f"arquivo de base externa nao encontrado: {base_externa_path}",
        }

    if not tema_sisab:
        return {
            "status": "reprovado",
            "erro": "informe --v6-tema (ex: sifg, sifc) ao usar --base-externa",
        }

    sisab_uf_ano: dict[tuple[str, int], int | float] = {}
    with gold_uf_path.open("r", encoding="utf-8-sig", newline="") as f_gold:
        leitor = csv.DictReader(f_gold)
        if tema_sisab not in (leitor.fieldnames or []):
            return {
                "status": "reprovado",
                "erro": f"tema '{tema_sisab}' nao existe em {gold_uf_path.name}",
            }
        for row in leitor:
            sisab_uf_ano[(row["uf"].strip().upper(), int(row["ano"]))] = parsear_numero(
                row[tema_sisab]
            )

    ext_uf_ano: dict[tuple[str, int], int | float] = defaultdict(int)
    with base_externa_path.open("r", encoding="utf-8-sig", newline="") as f_ext:
        amostra = f_ext.read(2048)
        f_ext.seek(0)
        delimitador = ";" if amostra.count(";") > amostra.count(",") else ","
        leitor_ext = csv.DictReader(f_ext, delimiter=delimitador)
        campos = leitor_ext.fieldnames or []
        for col in (col_uf, col_ano, col_valor):
            if col not in campos:
                return {
                    "status": "reprovado",
                    "erro": f"coluna '{col}' ausente na base externa ({', '.join(campos)})",
                }
        for row in leitor_ext:
            uf = (row[col_uf] or "").strip().upper()
            ano_txt = (row[col_ano] or "").strip()
            val_txt = (row[col_valor] or "").strip()
            if uf and ano_txt and val_txt:
                ext_uf_ano[(uf, int(ano_txt))] += parsear_numero(val_txt)

    chaves_comuns = sorted(set(sisab_uf_ano) & set(ext_uf_ano), key=lambda x: (x[1], x[0]))
    razoes_validas: list[float] = []
    linhas_comp: list[dict[str, object]] = []

    for uf, ano in chaves_comuns:
        v_sisab = sisab_uf_ano[(uf, ano)]
        v_ext = ext_uf_ano[(uf, ano)]
        razao = (v_sisab / v_ext) if v_ext else None
        if razao is not None:
            razoes_validas.append(razao)
        linhas_comp.append(
            {
                "uf": uf,
                "ano": ano,
                "tema_sisab": tema_sisab,
                "valor_sisab": v_sisab,
                "valor_externo": v_ext,
                "razao_sisab_sobre_externo": round(razao, 6) if razao is not None else "",
            }
        )

    mediana = statistics.median(razoes_validas) if razoes_validas else None
    desvios: list[dict[str, object]] = []
    for item in linhas_comp:
        razao = item["razao_sisab_sobre_externo"]
        flag_desvio = ""
        if razao == "":
            flag_desvio = "denominador_zero"
        elif mediana and mediana > 0 and (float(razao) > 2.5 * mediana or float(razao) < 0.4 * mediana):
            flag_desvio = "razao_fora_faixa_mediana"
        item["alerta_desvio"] = flag_desvio
        if flag_desvio:
            desvios.append(item)

    saida_csv_path.parent.mkdir(parents=True, exist_ok=True)
    with saida_csv_path.open("w", encoding="utf-8", newline="") as f_out:
        escritor = csv.DictWriter(
            f_out,
            fieldnames=[
                "uf",
                "ano",
                "tema_sisab",
                "valor_sisab",
                "valor_externo",
                "razao_sisab_sobre_externo",
                "alerta_desvio",
            ],
        )
        escritor.writeheader()
        escritor.writerows(linhas_comp)

    return {
        "status": "informado",
        "base_externa": formatar_caminho(base_externa_path, BASE_DIR),
        "tema_sisab": tema_sisab,
        "pares_uf_ano_comparados": len(chaves_comuns),
        "mediana_razao": round(mediana, 6) if mediana is not None else None,
        "qtd_desvios_sinalizados": len(desvios),
        "desvios_amostra": desvios[:20],
        "arquivo_comparacao_csv": formatar_caminho(saida_csv_path, BASE_DIR),
    }


def executar_validacoes(args: argparse.Namespace) -> int:
    # orquestra as verificacoes v1, v2, v4, v5 e v6 opcional sobre bronze, silver e gold
    for caminho, rotulo in [
        (args.registro, "registro_bronze.csv"),
        (args.silver_csv, "silver_sisab_valor.csv"),
        (args.gold_mun, "gold_sisab_municipio_ano.csv"),
        (args.gold_uf, "gold_sisab_uf_ano.csv"),
    ]:
        if not caminho.exists():
            print(f"[erro] arquivo obrigatorio nao encontrado ({rotulo}): {caminho}")
            return 1

    registros = carregar_registro(args.registro)

    try:
        municipios_ref = carregar_tabela_municipios(
            args.municipios_ref,
            baixar_se_ausente=not args.sem_download_ibge,
            forcar_download=args.baixar_municipios,
        )
    except (FileNotFoundError, urllib.error.URLError, ValueError) as exc:
        print(f"[erro] falha ao carregar tabela de municipios do ibge: {exc}")
        return 1

    with args.silver_csv.open("r", encoding="utf-8-sig", newline="") as f_silver:
        silver_rows = list(csv.DictReader(f_silver))

    v1 = validar_v1_registro_completo(args.bronze_dir, registros)
    v2 = validar_v2_soma_territorial(
        args.bronze_dir, registros, silver_rows, args.gold_mun, args.gold_uf
    )
    v3 = {
        "status": "ignorado",
        "motivo": "reproducao cruzada ignorada no escopo automatizado atual",
    }
    v4 = validar_v4_chaves_municipios(silver_rows, args.gold_mun, municipios_ref)
    v5 = validar_v5_cobertura(registros, silver_rows, len(municipios_ref), args.saida_cobertura)
    v6 = validar_v6_base_externa(
        gold_uf_path=args.gold_uf,
        base_externa_path=args.base_externa,
        tema_sisab=args.v6_tema,
        col_uf=args.v6_col_uf,
        col_ano=args.v6_col_ano,
        col_valor=args.v6_col_valor,
        saida_csv_path=args.saida_v6,
    )

    obrigatorias_ok = (
        v1["status"] == "aprovado"
        and v2["status"] == "aprovado"
        and v4["status"] == "aprovado"
        and v6["status"] != "reprovado"
    )

    relatorio = {
        "etapa": "E4_validar",
        "gerado_em_utc": datetime.now(timezone.utc).isoformat(),
        "status": "aprovado" if obrigatorias_ok else "reprovado",
        "validacoes": {
            "V1_registro_completo": v1,
            "V2_soma_territorial": v2,
            "V3_reproducao_cruzada": v3,
            "V4_chave_valida_ibge": v4,
            "V5_cobertura": v5,
            "V6_comparacao_base_externa": v6,
        },
    }

    args.saida_relatorio.parent.mkdir(parents=True, exist_ok=True)
    with args.saida_relatorio.open("w", encoding="utf-8") as arquivo:
        json.dump(relatorio, arquivo, ensure_ascii=False, indent=2)

    print("validacao e4 (silver e gold):")
    print(
        f"  V1 registro completo : {v1['status']} "
        f"({v1['arquivos_vinculados']}/{v1['arquivos_bronze_total']} arquivos | "
        f"{v1['lotes_com_arquivo']}/{v1['lotes_registrados_total']} lotes)"
    )
    print(
        f"  V2 soma territorial  : {v2['status']} "
        f"(div_bronze_silver={len(v2['divergencias_bronze_vs_silver'])}, "
        f"div_mun_uf={len(v2['divergencias_municipios_vs_uf'])}, "
        f"div_uf_br={len(v2['divergencias_uf_vs_brasil'])})"
    )
    print(f"  V3 reproducao cruzada: {v3['status']}")
    print(
        f"  V4 chave valida ibge : {v4['status']} "
        f"({v4['municipios_validos']}/{v4['municipios_unicos_nos_dados']} municipios = "
        f"{v4['pct_municipios_validos']:.2f}% | ref_ibge={v4['municipios_referencia_ibge']})"
    )
    if v4["excecoes_municipios_inexistentes"]:
        print(
            f"     excecoes ibge     : {', '.join(v4['excecoes_municipios_inexistentes'][:15])}"
        )
    if v4["excecoes_conflito_uf"]:
        print(f"     conflitos uf      : {len(v4['excecoes_conflito_uf'])}")

    print("  V5 cobertura por tema:")
    for item in v5["por_tema"]:
        print(
            f"     {item['tema']:<5} | "
            f"comps={item['competencias_com_valor']:>3}/{item['competencias_janela']} "
            f"(sem={item['competencias_sem_valor']:>2}) | "
            f"muns={item['municipios_com_valor']:>4}/{item['municipios_referencia']} "
            f"({item['pct_municipios_com_valor']:>5.1f}%) | "
            f"celulas_sem_valor={item['celulas_mun_comp_sem_valor']:>7} "
            f"({item['pct_celulas_sem_valor']:.2f}%)"
        )

    print(f"  V6 base externa      : {v6['status']}")
    if v6["status"] == "informado":
        print(
            f"     pares={v6['pares_uf_ano_comparados']} | "
            f"mediana_razao={v6['mediana_razao']} | "
            f"desvios={v6['qtd_desvios_sinalizados']}"
        )

    print(f"\ncobertura v5 salva em: {formatar_caminho(args.saida_cobertura, BASE_DIR)}")
    print(f"relatorio salvo em   : {formatar_caminho(args.saida_relatorio, BASE_DIR)}")
    print(f"status e4: {relatorio['status']}")
    return 0 if obrigatorias_ok else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="etapa e4: executa validacoes v1, v2, v4, v5 e v6 opcional sobre bronze, silver e gold."
    )
    parser.add_argument("--bronze-dir", type=Path, default=BRONZE_DIR)
    parser.add_argument("--registro", type=Path, default=REGISTRO_PATH)
    parser.add_argument("--silver-csv", type=Path, default=SILVER_CSV_PATH)
    parser.add_argument("--gold-mun", type=Path, default=GOLD_MUN_CSV_PATH)
    parser.add_argument("--gold-uf", type=Path, default=GOLD_UF_CSV_PATH)
    parser.add_argument("--municipios-ref", type=Path, default=MUNICIPIOS_REF_PATH)
    parser.add_argument("--saida-relatorio", type=Path, default=RELATORIO_E4_PATH)
    parser.add_argument("--saida-cobertura", type=Path, default=COBERTURA_V5_CSV_PATH)
    parser.add_argument("--saida-v6", type=Path, default=COMPARACAO_V6_CSV_PATH)
    parser.add_argument(
        "--baixar-municipios",
        action="store_true",
        help="baixa ou atualiza referencias/municipios_ibge.csv a partir da api oficial do ibge",
    )
    parser.add_argument(
        "--sem-download-ibge",
        action="store_true",
        help="impede download automatico da tabela do ibge se o arquivo local nao existir",
    )
    parser.add_argument(
        "--base-externa",
        type=Path,
        default=None,
        help="caminho opcional de csv externo (sinan ou sinasc) para executar a validacao v6",
    )
    parser.add_argument(
        "--v6-tema",
        type=str,
        default=None,
        help="coluna de tema na gold_sisab_uf_ano usada na comparacao v6 (ex: sifg)",
    )
    parser.add_argument("--v6-col-uf", type=str, default="uf")
    parser.add_argument("--v6-col-ano", type=str, default="ano")
    parser.add_argument("--v6-col-valor", type=str, default="valor")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return executar_validacoes(args)


if __name__ == "__main__":
    sys.exit(main())