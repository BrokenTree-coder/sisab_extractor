from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"
REGISTRO_PATH = BASE_DIR / "registro" / "registro_bronze.csv"
SILVER_DIR = BASE_DIR / "data" / "silver"
SILVER_CSV_PATH = SILVER_DIR / "silver_sisab_valor.csv"
RELATORIO_E2_PATH = SILVER_DIR / "relatorio_e2_silver.json"

PADRAO_ARQUIVO_LOTE = re.compile(
    r"^SISAB_(?P<tema>[a-z0-9_-]+)_(?P<competencia>\d{6})_(?P<id_consulta>[LJ]\d{4})\.csv$"
)
PADRAO_ID_SUFIXO = re.compile(r"_(?P<id_consulta>[LJ]\d{4})\.csv$")
PADRAO_CODIGO_PARENTE = re.compile(r"\((?P<codigo>[A-Z]\d{2}(?:\.?\d)?)\)", re.IGNORECASE)
PADRAO_CODIGO_INICIO = re.compile(
    r"^(?:cid|ciap)?\s*[-:]?\s*(?P<codigo>[A-Z]\d{2}(?:\.?\d)?)\b", re.IGNORECASE
)

MESES_PT = {
    "jan": 1,
    "fev": 2,
    "mar": 3,
    "abr": 4,
    "mai": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "set": 9,
    "out": 10,
    "nov": 11,
    "dez": 12,
}

COLUNAS_SILVER = [
    "id_consulta",
    "relatorio",
    "tema",
    "filtros",
    "nivel",
    "mun",
    "uf",
    "ano",
    "mes",
    "quadrimestre",
    "medida",
    "valor",
]


def normalizar_texto(texto: str) -> str:
    # remove acentos e espacos extras para comparacao consistente
    sem_acento = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    return " ".join(sem_acento.strip().lower().split())


def formatar_caminho(caminho: Path, base: Path) -> str:
    # exibe caminho relativo a raiz do projeto quando possivel
    try:
        return caminho.resolve().relative_to(base.resolve()).as_posix()
    except ValueError:
        return caminho.as_posix()


def montar_string_filtros(reg: dict[str, str]) -> str:
    # serializa os filtros aplicados na consulta em texto padronizado
    pares = [
        ("tipo_producao", reg.get("tipo_producao", "")),
        ("condicao_avaliada", reg.get("condicao_avaliada", "")),
        ("cid_ciap", reg.get("cid_ciap", "")),
        ("sexo", reg.get("filtro_sexo", "")),
        ("faixa_etaria", reg.get("filtro_faixa_etaria", "")),
        ("tipo_equipe", reg.get("filtro_tipo_equipe", "")),
        ("local_atendimento", reg.get("filtro_local_atendimento", "")),
        ("outros", reg.get("outros_filtros", "")),
    ]
    return " | ".join(f"{chave}={valor}" for chave, valor in pares if valor)


def calcular_quadrimestre(mes: int) -> str:
    # converte mes numerico para o quadrimestre padrao do sisab
    quad = (mes - 1) // 4 + 1
    return f"Q{quad}"


def extrair_nome_medida(coluna_bruta: str) -> str:
    # extrai codigo cid ou ciap quando presente no rotulo da coluna
    limpa = " ".join(coluna_bruta.strip().split())
    match_par = PADRAO_CODIGO_PARENTE.search(limpa)
    if match_par:
        return match_par.group("codigo").replace(".", "").upper()

    match_ini = PADRAO_CODIGO_INICIO.match(limpa)
    if match_ini:
        return match_ini.group("codigo").replace(".", "").upper()

    return limpa


def parsear_valor_numerico(bruto: str) -> int | float | None:
    # converte valor textual no padrao brasileiro para numero inteiro ou decimal
    texto = bruto.strip()
    if not texto or texto in {"-", "--", "na", "n/a", "null"}:
        return None

    # trata separador de milhar com ponto e decimal com virgula
    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    elif re.fullmatch(r"\d{1,3}(?:\.\d{3})+", texto):
        texto = texto.replace(".", "")

    try:
        numero = float(texto)
    except ValueError:
        return None

    if numero.is_integer():
        return int(numero)
    return numero


def extrair_competencia_cabecalho(linhas_cabecalho: list[str]) -> tuple[int, int] | None:
    # tenta ler ano e mes na linha de competencia do cabecalho do sisab
    for linha in linhas_cabecalho:
        norm = normalizar_texto(linha)
        if not norm.startswith("competencia"):
            continue
        match = re.search(r"([a-z]{3})\s*/\s*(\d{4})", norm)
        if match:
            mes = MESES_PT.get(match.group(1))
            ano = int(match.group(2))
            if mes is not None:
                return ano, mes
    return None


def carregar_registro(caminho_registro: Path) -> dict[str, dict[str, str]]:
    # carrega o registro da bronze indexado por id_consulta
    if not caminho_registro.exists():
        raise FileNotFoundError(f"arquivo de registro nao encontrado: {caminho_registro}")

    registros: dict[str, dict[str, str]] = {}
    with caminho_registro.open("r", encoding="utf-8-sig", newline="") as arquivo:
        for linha in csv.DictReader(arquivo):
            id_consulta = (linha.get("id_consulta") or "").strip()
            if id_consulta:
                registros[id_consulta] = {k: (v or "").strip() for k, v in linha.items()}
    return registros


def processar_arquivo_bronze(
    caminho: Path,
    reg: dict[str, str],
    manter_zeros: bool = False,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    # remove titulo e rodape do csv bruto e transforma as medidas para formato longo
    nome = caminho.name
    match_lote = PADRAO_ARQUIVO_LOTE.match(nome)

    with caminho.open("r", encoding="latin-1", errors="replace") as arquivo:
        linhas = [linha.rstrip("\r\n") for linha in arquivo]

    idx_cabecalho = None
    for idx, linha in enumerate(linhas):
        if ";" not in linha:
            continue
        colunas_norm = [normalizar_texto(c) for c in linha.split(";")]
        if "ibge" in colunas_norm and ("municipio" in colunas_norm or "uf" in colunas_norm):
            idx_cabecalho = idx
            break

    if idx_cabecalho is None:
        raise ValueError(f"cabecalho tabular nao encontrado em {nome}")

    comp_cabecalho = extrair_competencia_cabecalho(linhas[:idx_cabecalho])
    if match_lote:
        comp_raw = match_lote.group("competencia")
        ano = int(comp_raw[:4])
        mes = int(comp_raw[4:6])
        if comp_cabecalho is not None and comp_cabecalho != (ano, mes):
            raise ValueError(
                f"competencia divergente em {nome}: arquivo={ano:04d}-{mes:02d}, cabecalho={comp_cabecalho}"
            )
    elif comp_cabecalho is not None:
        ano, mes = comp_cabecalho
    else:
        raise ValueError(f"nao foi possivel determinar competencia (ano/mes) de {nome}")

    quadrimestre = calcular_quadrimestre(mes)

    # remove delimitador sobrando no final da linha de cabecalho
    cabecalho_raw = linhas[idx_cabecalho].split(";")
    while cabecalho_raw and not cabecalho_raw[-1].strip():
        cabecalho_raw.pop()

    colunas_norm = [normalizar_texto(c) for c in cabecalho_raw]
    idx_uf = colunas_norm.index("uf") if "uf" in colunas_norm else None
    idx_ibge = colunas_norm.index("ibge") if "ibge" in colunas_norm else None
    idx_mun_nome = colunas_norm.index("municipio") if "municipio" in colunas_norm else None

    if idx_ibge is None:
        raise ValueError(f"coluna ibge ausente no cabecalho de {nome}")

    indices_fixos = {i for i in (idx_uf, idx_ibge, idx_mun_nome) if i is not None}
    indices_medidas: list[tuple[int, str]] = []
    indice_total: tuple[int, str] | None = None

    for idx_col, nome_col in enumerate(cabecalho_raw):
        if idx_col in indices_fixos:
            continue
        col_limpa = nome_col.strip()
        if not col_limpa:
            continue
        if normalizar_texto(col_limpa) == "total":
            indice_total = (idx_col, "TOTAL")
            continue
        indices_medidas.append((idx_col, extrair_nome_medida(col_limpa)))

    # usa a coluna total apenas quando nao existir nenhuma outra coluna de medida
    if not indices_medidas and indice_total is not None:
        indices_medidas.append(indice_total)

    if not indices_medidas:
        raise ValueError(f"nenhuma coluna de medida encontrada em {nome}")

    filtros_str = montar_string_filtros(reg)
    nivel_norm = normalizar_texto(reg.get("nivel_geografico", "municipio"))
    relatorio = reg.get("relatorio", "")
    tema = reg.get("tema", "")
    id_consulta = reg.get("id_consulta", "")

    registros_longos: list[dict[str, object]] = []
    linhas_dados_lidas = 0
    zeros_ignorados = 0
    valores_invalidos = 0

    for linha in linhas[idx_cabecalho + 1 :]:
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
            continue
        if idx_ibge >= len(partes):
            continue

        ibge_bruto = partes[idx_ibge].strip()
        if not (ibge_bruto.isdigit() and len(ibge_bruto) in (6, 7)):
            continue

        # garante chave municipal padronizada de seis digitos
        mun = ibge_bruto[:6]
        uf = partes[idx_uf].strip().upper() if (idx_uf is not None and idx_uf < len(partes)) else ""
        linhas_dados_lidas += 1

        for idx_col, medida in indices_medidas:
            if idx_col >= len(partes):
                continue
            valor = parsear_valor_numerico(partes[idx_col])
            if valor is None:
                if partes[idx_col].strip() not in {"", "-", "--"}:
                    valores_invalidos += 1
                continue
            if valor == 0 and not manter_zeros:
                zeros_ignorados += 1
                continue

            registros_longos.append(
                {
                    "id_consulta": id_consulta,
                    "relatorio": relatorio,
                    "tema": tema,
                    "filtros": filtros_str,
                    "nivel": nivel_norm,
                    "mun": mun,
                    "uf": uf,
                    "ano": ano,
                    "mes": mes,
                    "quadrimestre": quadrimestre,
                    "medida": medida,
                    "valor": valor,
                }
            )

    metricas = {
        "linhas_dados_lidas": linhas_dados_lidas,
        "linhas_silver_geradas": len(registros_longos),
        "zeros_ignorados": zeros_ignorados,
        "valores_invalidos": valores_invalidos,
        "medidas": [m for _, m in indices_medidas],
    }
    return registros_longos, metricas


def padronizar_bronze_para_silver(
    bronze_dir: Path,
    registro_path: Path,
    silver_csv_path: Path,
    relatorio_path: Path,
    manter_zeros: bool = False,
    gerar_parquet: bool = False,
) -> int:
    # percorre todos os csvs da bronze e consolida a tabela longa silver_sisab_valor
    registros = carregar_registro(registro_path)
    arquivos_csv = sorted(bronze_dir.rglob("*.csv"))
    if not arquivos_csv:
        print(f"[erro] nenhum arquivo csv encontrado em {bronze_dir}")
        return 1

    silver_csv_path.parent.mkdir(parents=True, exist_ok=True)

    total_arquivos = 0
    total_linhas_bronze = 0
    total_linhas_silver = 0
    total_zeros_ignorados = 0
    total_valores_invalidos = 0
    resumo_por_tema: dict[str, dict[str, object]] = {}
    erros: list[str] = []

    with silver_csv_path.open("w", encoding="utf-8", newline="") as saida_csv:
        escritor = csv.DictWriter(saida_csv, fieldnames=COLUNAS_SILVER)
        escritor.writeheader()

        for caminho in arquivos_csv:
            match_sufixo = PADRAO_ID_SUFIXO.search(caminho.name)
            if not match_sufixo:
                erros.append(f"arquivo sem id_consulta no nome: {formatar_caminho(caminho, BASE_DIR)}")
                continue

            id_consulta = match_sufixo.group("id_consulta")
            if id_consulta not in registros:
                erros.append(
                    f"id_consulta {id_consulta} nao encontrado no registro: {formatar_caminho(caminho, BASE_DIR)}"
                )
                continue

            reg = registros[id_consulta]
            tema = reg["tema"]

            try:
                linhas_longas, metricas = processar_arquivo_bronze(
                    caminho=caminho,
                    reg=reg,
                    manter_zeros=manter_zeros,
                )
            except ValueError as exc:
                erros.append(str(exc))
                continue

            escritor.writerows(linhas_longas)

            total_arquivos += 1
            total_linhas_bronze += int(metricas["linhas_dados_lidas"])
            total_linhas_silver += int(metricas["linhas_silver_geradas"])
            total_zeros_ignorados += int(metricas["zeros_ignorados"])
            total_valores_invalidos += int(metricas["valores_invalidos"])

            stats = resumo_por_tema.setdefault(
                tema,
                {
                    "id_consulta": id_consulta,
                    "arquivos_processados": 0,
                    "linhas_bronze": 0,
                    "linhas_silver": 0,
                    "soma_valor": 0,
                    "medidas_unicas": set(),
                    "municipios_unicos": set(),
                    "ufs_unicas": set(),
                },
            )
            stats["arquivos_processados"] = int(stats["arquivos_processados"]) + 1
            stats["linhas_bronze"] = int(stats["linhas_bronze"]) + int(metricas["linhas_dados_lidas"])
            stats["linhas_silver"] = int(stats["linhas_silver"]) + int(metricas["linhas_silver_geradas"])
            stats["soma_valor"] = stats["soma_valor"] + sum(linha["valor"] for linha in linhas_longas)
            stats["medidas_unicas"].update(linha["medida"] for linha in linhas_longas)
            stats["municipios_unicos"].update(linha["mun"] for linha in linhas_longas)
            stats["ufs_unicas"].update(linha["uf"] for linha in linhas_longas if linha["uf"])

    parquet_gerado = None
    if gerar_parquet and not erros:
        try:
            import pandas as pd

            parquet_path = silver_csv_path.with_suffix(".parquet")
            df = pd.read_csv(silver_csv_path, dtype={"mun": str, "uf": str})
            df.to_parquet(parquet_path, index=False)
            parquet_gerado = formatar_caminho(parquet_path, BASE_DIR)
        except Exception as exc:
            erros.append(f"falha ao gerar parquet: {exc}")

    resumo_temas_json = []
    for tema, stats in sorted(resumo_por_tema.items()):
        resumo_temas_json.append(
            {
                "tema": tema,
                "id_consulta": stats["id_consulta"],
                "arquivos_processados": stats["arquivos_processados"],
                "linhas_bronze": stats["linhas_bronze"],
                "linhas_silver": stats["linhas_silver"],
                "soma_valor": stats["soma_valor"],
                "municipios_unicos": len(stats["municipios_unicos"]),
                "ufs_unicas": len(stats["ufs_unicas"]),
                "medidas": sorted(stats["medidas_unicas"]),
            }
        )

    relatorio = {
        "etapa": "E2_padronizar_silver",
        "gerado_em_utc": datetime.now(timezone.utc).isoformat(),
        "status": "aprovado" if not erros else "reprovado",
        "parametros": {
            "manter_zeros": manter_zeros,
            "saida_csv": formatar_caminho(silver_csv_path, BASE_DIR),
            "saida_parquet": parquet_gerado,
        },
        "totais": {
            "arquivos_processados": total_arquivos,
            "linhas_bronze_lidas": total_linhas_bronze,
            "linhas_silver_geradas": total_linhas_silver,
            "zeros_ignorados": total_zeros_ignorados,
            "valores_invalidos": total_valores_invalidos,
            "erros": len(erros),
        },
        "temas": resumo_temas_json,
        "erros": erros,
    }
    with relatorio_path.open("w", encoding="utf-8") as arquivo:
        json.dump(relatorio, arquivo, ensure_ascii=False, indent=2)

    print("padronizacao e2 (bronze -> silver_sisab_valor):")
    print(f"  arquivos processados : {total_arquivos}")
    print(f"  linhas bronze lidas  : {total_linhas_bronze}")
    print(f"  linhas silver geradas: {total_linhas_silver}")
    print(f"  zeros ignorados      : {total_zeros_ignorados}")
    print(f"  valores invalidos    : {total_valores_invalidos}")
    print()
    print("resumo por tema:")
    for item in resumo_temas_json:
        print(
            f"  {item['id_consulta']} ({item['tema']:<4}) | "
            f"csvs={item['arquivos_processados']:>3} | "
            f"silver={item['linhas_silver']:>6} | "
            f"atendimentos={item['soma_valor']:>7} | "
            f"muns={item['municipios_unicos']:>4} | "
            f"medidas={','.join(item['medidas'])}"
        )

    if erros:
        print("\nerros encontrados:")
        for erro in erros:
            print(f"  [erro] {erro}")
        return 1

    print(f"\nsilver salva em   : {formatar_caminho(silver_csv_path, BASE_DIR)}")
    if parquet_gerado:
        print(f"parquet salvo em  : {parquet_gerado}")
    print(f"relatorio salvo em: {formatar_caminho(relatorio_path, BASE_DIR)}")
    print("status e2: aprovado")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="etapa e2: padroniza csvs da bronze para o formato longo silver_sisab_valor."
    )
    parser.add_argument("--bronze-dir", type=Path, default=BRONZE_DIR)
    parser.add_argument("--registro", type=Path, default=REGISTRO_PATH)
    parser.add_argument("--saida-csv", type=Path, default=SILVER_CSV_PATH)
    parser.add_argument("--saida-relatorio", type=Path, default=RELATORIO_E2_PATH)
    parser.add_argument(
        "--manter-zeros",
        action="store_true",
        help="mantem celulas com valor zero na tabela longa",
    )
    parser.add_argument(
        "--parquet",
        action="store_true",
        help="gera copia adicional em formato parquet ao lado do csv",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return padronizar_bronze_para_silver(
        bronze_dir=args.bronze_dir,
        registro_path=args.registro,
        silver_csv_path=args.saida_csv,
        relatorio_path=args.saida_relatorio,
        manter_zeros=args.manter_zeros,
        gerar_parquet=args.parquet,
    )


if __name__ == "__main__":
    sys.exit(main())