from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
SILVER_CSV_PATH = BASE_DIR / "data" / "silver" / "silver_sisab_valor.csv"
GOLD_DIR = BASE_DIR / "data" / "gold"
GOLD_MUN_CSV_PATH = GOLD_DIR / "gold_sisab_municipio_ano.csv"
GOLD_UF_CSV_PATH = GOLD_DIR / "gold_sisab_uf_ano.csv"
RELATORIO_E3_PATH = GOLD_DIR / "relatorio_e3_gold.json"

COLUNAS_OBRIGATORIAS_SILVER = [
    "id_consulta",
    "tema",
    "mun",
    "uf",
    "ano",
    "mes",
    "quadrimestre",
    "medida",
    "valor",
]


def formatar_caminho(caminho: Path, base: Path) -> str:
    # exibe caminho relativo a raiz do projeto quando possivel
    try:
        return caminho.resolve().relative_to(base.resolve()).as_posix()
    except ValueError:
        return caminho.as_posix()


def sha256_arquivo(caminho: Path) -> str:
    # calcula hash sha-256 em blocos para rastreabilidade dos arquivos gold
    hasher = hashlib.sha256()
    with caminho.open("rb") as arquivo:
        for bloco in iter(lambda: arquivo.read(65536), b""):
            hasher.update(bloco)
    return hasher.hexdigest()


def parsear_numero(texto: str) -> int | float:
    # converte valor textual da silver preservando inteiros quando possivel
    limpo = texto.strip()
    numero = float(limpo)
    if numero.is_integer():
        return int(numero)
    return numero


def formatar_numero(valor: int | float) -> int | float:
    # evita casas decimais desnecessarias em somas inteiras
    if isinstance(valor, float) and valor.is_integer():
        return int(valor)
    return valor


def agregar_silver_para_gold(
    silver_csv_path: Path,
    gold_mun_csv_path: Path,
    gold_uf_csv_path: Path,
    relatorio_path: Path,
    prefixo_indicador: str = "",
    gerar_parquet: bool = False,
) -> int:
    # agrega a tabela longa da silver em paineis anuais largos por municipio e por uf
    if not silver_csv_path.exists():
        print(f"[erro] arquivo silver nao encontrado: {silver_csv_path}")
        return 1

    erros: list[str] = []
    avisos: list[str] = []

    soma_mun_ano: dict[tuple[str, str, int], dict[str, int | float]] = defaultdict(
        lambda: defaultdict(int)
    )
    soma_uf_ano: dict[tuple[str, int], dict[str, int | float]] = defaultdict(
        lambda: defaultdict(int)
    )
    totais_silver_tema: dict[str, int | float] = defaultdict(int)
    ufs_por_mun: dict[str, set[str]] = defaultdict(set)
    anos_encontrados: set[int] = set()
    temas_encontrados: set[str] = set()
    linhas_silver_lidas = 0

    with silver_csv_path.open("r", encoding="utf-8-sig", newline="") as arquivo:
        leitor = csv.DictReader(arquivo)
        colunas = leitor.fieldnames or []
        faltantes = [c for c in COLUNAS_OBRIGATORIAS_SILVER if c not in colunas]
        if faltantes:
            print(f"[erro] colunas obrigatorias ausentes na silver: {', '.join(faltantes)}")
            return 1

        for num_linha, linha in enumerate(leitor, start=2):
            tema = (linha.get("tema") or "").strip()
            mun = (linha.get("mun") or "").strip()
            uf = (linha.get("uf") or "").strip().upper()
            ano_txt = (linha.get("ano") or "").strip()
            valor_txt = (linha.get("valor") or "").strip()

            if not (tema and mun and uf and ano_txt and valor_txt):
                erros.append(f"linha {num_linha} da silver com campos obrigatorios vazios")
                continue

            try:
                ano = int(ano_txt)
                valor = parsear_numero(valor_txt)
            except ValueError:
                erros.append(
                    f"linha {num_linha} da silver com ano ou valor invalido: ano={ano_txt!r}, valor={valor_txt!r}"
                )
                continue

            linhas_silver_lidas += 1
            temas_encontrados.add(tema)
            anos_encontrados.add(ano)
            ufs_por_mun[mun].add(uf)

            soma_mun_ano[(mun, uf, ano)][tema] += valor
            soma_uf_ano[(uf, ano)][tema] += valor
            totais_silver_tema[tema] += valor

    if linhas_silver_lidas == 0:
        erros.append("nenhuma linha valida lida da silver")

    # verifica se algum codigo de municipio aparece associado a mais de uma uf
    for mun, ufs in sorted(ufs_por_mun.items()):
        if len(ufs) > 1:
            avisos.append(
                f"municipio {mun} aparece associado a multiplas ufs na silver: {', '.join(sorted(ufs))}"
            )

    if erros:
        for erro in erros[:20]:
            print(f"[erro] {erro}")
        if len(erros) > 20:
            print(f"[erro] ... e mais {len(erros) - 20} erro(s)")
        return 1

    temas_ordenados = sorted(temas_encontrados)
    mapa_colunas = {
        tema: f"{prefixo_indicador}{tema}" if prefixo_indicador else tema
        for tema in temas_ordenados
    }
    colunas_indicadores = [mapa_colunas[tema] for tema in temas_ordenados]

    gold_mun_csv_path.parent.mkdir(parents=True, exist_ok=True)
    gold_uf_csv_path.parent.mkdir(parents=True, exist_ok=True)

    # grava gold_sisab_municipio_ano em formato largo com um indicador por coluna
    colunas_gold_mun = ["mun", "uf", "ano"] + colunas_indicadores
    chaves_mun_ordenadas = sorted(soma_mun_ano.keys(), key=lambda x: (x[2], x[1], x[0]))
    totais_gold_mun_tema: dict[str, int | float] = defaultdict(int)

    with gold_mun_csv_path.open("w", encoding="utf-8", newline="") as saida_mun:
        escritor = csv.DictWriter(saida_mun, fieldnames=colunas_gold_mun)
        escritor.writeheader()
        for mun, uf, ano in chaves_mun_ordenadas:
            valores_chave = soma_mun_ano[(mun, uf, ano)]
            registro = {"mun": mun, "uf": uf, "ano": ano}
            for tema in temas_ordenados:
                val = formatar_numero(valores_chave.get(tema, 0))
                registro[mapa_colunas[tema]] = val
                totais_gold_mun_tema[tema] += val
            escritor.writerow(registro)

    # grava gold_sisab_uf_ano em formato largo com um indicador por coluna
    colunas_gold_uf = ["uf", "ano"] + colunas_indicadores
    chaves_uf_ordenadas = sorted(soma_uf_ano.keys(), key=lambda x: (x[1], x[0]))
    totais_gold_uf_tema: dict[str, int | float] = defaultdict(int)

    with gold_uf_csv_path.open("w", encoding="utf-8", newline="") as saida_uf:
        escritor = csv.DictWriter(saida_uf, fieldnames=colunas_gold_uf)
        escritor.writeheader()
        for uf, ano in chaves_uf_ordenadas:
            valores_chave = soma_uf_ano[(uf, ano)]
            registro = {"uf": uf, "ano": ano}
            for tema in temas_ordenados:
                val = formatar_numero(valores_chave.get(tema, 0))
                registro[mapa_colunas[tema]] = val
                totais_gold_uf_tema[tema] += val
            escritor.writerow(registro)

    # confere conservacao exata das somas entre silver, gold municipal e gold estadual
    resumo_indicadores = []
    for tema in temas_ordenados:
        total_s = formatar_numero(totais_silver_tema[tema])
        total_m = formatar_numero(totais_gold_mun_tema[tema])
        total_u = formatar_numero(totais_gold_uf_tema[tema])
        if total_s != total_m or total_s != total_u:
            erros.append(
                f"divergencia de soma no indicador {tema}: silver={total_s}, gold_mun={total_m}, gold_uf={total_u}"
            )
        resumo_indicadores.append(
            {
                "tema": tema,
                "coluna_gold": mapa_colunas[tema],
                "soma_silver": total_s,
                "soma_gold_municipio_ano": total_m,
                "soma_gold_uf_ano": total_u,
                "conciliado": (total_s == total_m == total_u),
            }
        )

    parquets_gerados: dict[str, str | None] = {
        "gold_municipio_ano": None,
        "gold_uf_ano": None,
    }
    if gerar_parquet and not erros:
        try:
            import pandas as pd

            pq_mun = gold_mun_csv_path.with_suffix(".parquet")
            pq_uf = gold_uf_csv_path.with_suffix(".parquet")
            pd.read_csv(gold_mun_csv_path, dtype={"mun": str, "uf": str}).to_parquet(
                pq_mun, index=False
            )
            pd.read_csv(gold_uf_csv_path, dtype={"uf": str}).to_parquet(pq_uf, index=False)
            parquets_gerados["gold_municipio_ano"] = formatar_caminho(pq_mun, BASE_DIR)
            parquets_gerados["gold_uf_ano"] = formatar_caminho(pq_uf, BASE_DIR)
        except Exception as exc:
            erros.append(f"falha ao gerar parquet na gold: {exc}")

    relatorio = {
        "etapa": "E3_agregar_gold",
        "gerado_em_utc": datetime.now(timezone.utc).isoformat(),
        "status": "aprovado" if not erros else "reprovado",
        "entrada": {
            "silver_csv": formatar_caminho(silver_csv_path, BASE_DIR),
            "linhas_silver_lidas": linhas_silver_lidas,
        },
        "saidas": {
            "gold_municipio_ano": {
                "caminho": formatar_caminho(gold_mun_csv_path, BASE_DIR),
                "parquet": parquets_gerados["gold_municipio_ano"],
                "n_linhas": len(chaves_mun_ordenadas),
                "colunas": colunas_gold_mun,
                "sha256": sha256_arquivo(gold_mun_csv_path),
            },
            "gold_uf_ano": {
                "caminho": formatar_caminho(gold_uf_csv_path, BASE_DIR),
                "parquet": parquets_gerados["gold_uf_ano"],
                "n_linhas": len(chaves_uf_ordenadas),
                "colunas": colunas_gold_uf,
                "sha256": sha256_arquivo(gold_uf_csv_path),
            },
        },
        "cobertura": {
            "ano_min": min(anos_encontrados) if anos_encontrados else None,
            "ano_max": max(anos_encontrados) if anos_encontrados else None,
            "anos_unicos": len(anos_encontrados),
            "municipios_unicos": len(ufs_por_mun),
            "ufs_unicas": len({uf for _, uf, _ in chaves_mun_ordenadas}),
        },
        "indicadores": resumo_indicadores,
        "avisos": avisos,
        "erros": erros,
    }
    with relatorio_path.open("w", encoding="utf-8") as arquivo:
        json.dump(relatorio, arquivo, ensure_ascii=False, indent=2)

    print("agregacao e3 (silver -> gold):")
    print(f"  linhas silver lidas      : {linhas_silver_lidas}")
    print(
        f"  gold_sisab_municipio_ano : {len(chaves_mun_ordenadas)} linhas ({len(ufs_por_mun)} municipios unicos)"
    )
    print(
        f"  gold_sisab_uf_ano        : {len(chaves_uf_ordenadas)} linhas ({len({uf for _, uf, _ in chaves_mun_ordenadas})} ufs unicas)"
    )
    print(
        f"  janela anual             : {min(anos_encontrados)} a {max(anos_encontrados)} ({len(anos_encontrados)} anos)"
    )
    print(f"  colunas de indicadores   : {', '.join(colunas_indicadores)}")
    print()
    print("conferencia de somas (silver == gold_mun == gold_uf):")
    for ind in resumo_indicadores:
        status_ind = "ok" if ind["conciliado"] else "divergente"
        print(
            f"  {ind['coluna_gold']:<12} | "
            f"silver={ind['soma_silver']:>7} | "
            f"mun_ano={ind['soma_gold_municipio_ano']:>7} | "
            f"uf_ano={ind['soma_gold_uf_ano']:>7} | "
            f"{status_ind}"
        )

    if avisos:
        print("\navisos:")
        for aviso in avisos:
            print(f"  [aviso] {aviso}")

    if erros:
        print("\nerros encontrados:")
        for erro in erros:
            print(f"  [erro] {erro}")
        return 1

    print(f"\ngold municipal salva em : {formatar_caminho(gold_mun_csv_path, BASE_DIR)}")
    print(f"gold estadual salva em  : {formatar_caminho(gold_uf_csv_path, BASE_DIR)}")
    print(f"relatorio salvo em      : {formatar_caminho(relatorio_path, BASE_DIR)}")
    print("status e3: aprovado")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="etapa e3: agrega silver_sisab_valor em gold_sisab_municipio_ano e gold_sisab_uf_ano."
    )
    parser.add_argument("--silver-csv", type=Path, default=SILVER_CSV_PATH)
    parser.add_argument("--saida-mun", type=Path, default=GOLD_MUN_CSV_PATH)
    parser.add_argument("--saida-uf", type=Path, default=GOLD_UF_CSV_PATH)
    parser.add_argument("--saida-relatorio", type=Path, default=RELATORIO_E3_PATH)
    parser.add_argument(
        "--prefixo-indicador",
        type=str,
        default="",
        help="prefixo opcional para os nomes das colunas de indicadores na gold (ex: sisab_)",
    )
    parser.add_argument(
        "--parquet",
        action="store_true",
        help="gera copias adicionais em formato parquet ao lado dos csvs da gold",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return agregar_silver_para_gold(
        silver_csv_path=args.silver_csv,
        gold_mun_csv_path=args.saida_mun,
        gold_uf_csv_path=args.saida_uf,
        relatorio_path=args.saida_relatorio,
        prefixo_indicador=args.prefixo_indicador,
        gerar_parquet=args.parquet,
    )


if __name__ == "__main__":
    sys.exit(main())