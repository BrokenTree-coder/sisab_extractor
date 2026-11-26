from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sqlite3
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"
REGISTRO_PATH = BASE_DIR / "registro" / "registro_bronze.csv"
CHECKPOINT_PATH = BASE_DIR / "data" / "checkpoint.db"
MANIFESTO_CSV_PATH = BASE_DIR / "registro" / "manifesto_bronze.csv"
RELATORIO_JSON_PATH = BASE_DIR / "registro" / "relatorio_e1_bronze.json"

PADRAO_ARQUIVO_LOTE = re.compile(
    r"^SISAB_(?P<tema>[a-z0-9_-]+)_(?P<competencia>\d{6})_(?P<id_consulta>[LJ]\d{4})\.csv$"
)
PADRAO_ID_SUFIXO = re.compile(r"_(?P<id_consulta>[LJ]\d{4})\.csv$")

COLUNAS_OBRIGATORIAS_REGISTRO = [
    "id_consulta",
    "pessoa",
    "data_hora_consulta",
    "relatorio",
    "url_relatorio",
    "tipo_producao",
    "tema",
    "condicao_avaliada",
    "cid_ciap",
    "nivel_geografico",
    "recorte_territorial",
    "competencia_inicio",
    "competencia_fim",
    "linha",
    "coluna",
    "filtro_sexo",
    "filtro_faixa_etaria",
    "filtro_tipo_equipe",
    "filtro_local_atendimento",
    "outros_filtros",
    "formato_exportado",
    "nome_arquivo_agregado",
]


def normalizar_texto(texto: str) -> str:
    # remove acentos e espacos excedentes para comparacao robusta
    sem_acento = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    return " ".join(sem_acento.strip().lower().split())


def formatar_caminho(caminho: Path, base: Path) -> str:
    # tenta exibir caminho relativo a raiz do projeto sem quebrar para caminhos externos
    try:
        return caminho.resolve().relative_to(base.resolve()).as_posix()
    except ValueError:
        return caminho.as_posix()


def sha256_arquivo(caminho: Path) -> str:
    # calcula hash sha-256 em blocos para nao sobrecarregar memoria
    hasher = hashlib.sha256()
    with caminho.open("rb") as arquivo:
        for bloco in iter(lambda: arquivo.read(65536), b""):
            hasher.update(bloco)
    return hasher.hexdigest()


def sha256_lote(pares_ordenados: list[tuple[str, str]]) -> str:
    # gera hash deterministico do lote a partir dos nomes e hashes dos arquivos mensais
    hasher = hashlib.sha256()
    for caminho_rel, digest in sorted(pares_ordenados):
        hasher.update(f"{caminho_rel}:{digest}\n".encode("utf-8"))
    return hasher.hexdigest()


def gerar_competencias_esperadas(inicio: str, fim: str) -> list[str]:
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


def inspecionar_csv_bronze(caminho: Path) -> dict[str, object]:
    # le o csv bruto em latin-1 e verifica cabecalho tabular, rodape e contagem de linhas de dados
    tamanho_bytes = caminho.stat().st_size
    if tamanho_bytes == 0:
        return {
            "tamanho_bytes": 0,
            "n_linhas_dados": 0,
            "tem_cabecalho_tabela": False,
            "tem_rodape_fonte": False,
            "erro_estrutura": "arquivo vazio (0 bytes)",
        }

    with caminho.open("r", encoding="latin-1", errors="replace") as arquivo:
        linhas = [linha.rstrip("\r\n") for linha in arquivo]

    idx_cabecalho = None
    tem_rodape = False

    for idx, linha in enumerate(linhas):
        norm = normalizar_texto(linha)
        if idx_cabecalho is None and ";" in linha:
            colunas = [normalizar_texto(c) for c in linha.split(";")]
            if "ibge" in colunas and ("municipio" in colunas or "uf" in colunas):
                idx_cabecalho = idx
                continue
        if norm.startswith("fonte:"):
            tem_rodape = True
            break

    n_linhas_dados = 0
    if idx_cabecalho is not None:
        for linha in linhas[idx_cabecalho + 1 :]:
            stripada = linha.strip()
            if not stripada:
                continue
            norm = normalizar_texto(stripada)
            if norm.startswith("fonte:") or norm.startswith("dado gerado em:"):
                tem_rodape = True
                break
            partes = [p.strip() for p in stripada.split(";")]
            if not partes:
                continue
            primeira = normalizar_texto(partes[0])
            if primeira.startswith("total"):
                continue
            # confirma presenca de codigo ibge de 6 ou 7 digitos nas primeiras colunas
            if any(p.isdigit() and len(p) in (6, 7) for p in partes[:3]):
                n_linhas_dados += 1

    erros = []
    if idx_cabecalho is None:
        erros.append("cabecalho tabular (uf/ibge/municipio) nao encontrado")
    if not tem_rodape:
        erros.append("rodape oficial (fonte:) ausente")
    if n_linhas_dados == 0:
        erros.append("nenhuma linha de dados municipal encontrada")

    return {
        "tamanho_bytes": tamanho_bytes,
        "n_linhas_dados": n_linhas_dados,
        "tem_cabecalho_tabela": idx_cabecalho is not None,
        "tem_rodape_fonte": tem_rodape,
        "erro_estrutura": "; ".join(erros),
    }


def carregar_registro(caminho_registro: Path) -> tuple[dict[str, dict[str, str]], list[str]]:
    # carrega o registro da camada bronze e valida colunas obrigatorias e unicidade de id
    erros: list[str] = []
    if not caminho_registro.exists():
        return {}, [f"arquivo de registro nao encontrado: {caminho_registro}"]

    registros: dict[str, dict[str, str]] = {}
    with caminho_registro.open("r", encoding="utf-8-sig", newline="") as arquivo:
        leitor = csv.DictReader(arquivo)
        colunas = leitor.fieldnames or []
        faltantes = [c for c in COLUNAS_OBRIGATORIAS_REGISTRO if c not in colunas]
        if faltantes:
            erros.append(f"colunas obrigatorias ausentes no registro: {', '.join(faltantes)}")
            return {}, erros

        for num_linha, linha in enumerate(leitor, start=2):
            id_consulta = (linha.get("id_consulta") or "").strip()
            if not id_consulta:
                erros.append(f"linha {num_linha} do registro sem id_consulta")
                continue
            if id_consulta in registros:
                erros.append(f"id_consulta duplicado no registro: {id_consulta}")
                continue

            campos_vazios = [
                col for col in COLUNAS_OBRIGATORIAS_REGISTRO if not (linha.get(col) or "").strip()
            ]
            if campos_vazios:
                erros.append(
                    f"registro {id_consulta} (linha {num_linha}) com campos obrigatorios vazios: {', '.join(campos_vazios)}"
                )

            registros[id_consulta] = {k: (v or "").strip() for k, v in linha.items()}

    if not registros:
        erros.append("registro de consultas esta vazio")

    return registros, erros


def carregar_resumo_checkpoint(caminho_db: Path) -> dict[str, dict[str, int]] | None:
    # consulta opcional ao sqlite local para cruzar baixados e meses no_data
    if not caminho_db.exists():
        return None

    resumo: dict[str, dict[str, int]] = {}
    with sqlite3.connect(caminho_db) as conn:
        rows = conn.execute(
            "SELECT modulo, status, COUNT(*) FROM extracoes GROUP BY modulo, status"
        ).fetchall()
    for modulo, status, qtd in rows:
        resumo.setdefault(modulo, {})[status] = int(qtd)
    return resumo


def conferir_bronze(
    bronze_dir: Path,
    registro_path: Path,
    checkpoint_path: Path,
    manifesto_csv_path: Path,
    relatorio_json_path: Path,
    salvar_saidas: bool = True,
) -> int:
    # executa a conciliacao completa entre arquivos da bronze e o registro de consultas
    erros: list[str] = []
    avisos: list[str] = []

    registros, erros_registro = carregar_registro(registro_path)
    erros.extend(erros_registro)

    if not bronze_dir.exists():
        erros.append(f"diretorio bronze nao encontrado: {bronze_dir}")

    if erros and not registros:
        for erro in erros:
            print(f"[erro] {erro}")
        return 1

    # verifica arquivos temporarios residuais na bronze
    temporarios = sorted(bronze_dir.rglob("*.tmp")) if bronze_dir.exists() else []
    for tmp in temporarios:
        erros.append(f"arquivo temporario residual na bronze: {formatar_caminho(tmp, BASE_DIR)}")

    arquivos_csv = sorted(bronze_dir.rglob("*.csv")) if bronze_dir.exists() else []
    arquivos_por_lote: dict[str, list[dict[str, object]]] = {
        id_c: [] for id_c in registros
    }
    manifesto_linhas: list[dict[str, object]] = []

    for caminho in arquivos_csv:
        caminho_rel = formatar_caminho(caminho, BASE_DIR)
        nome = caminho.name
        pasta_pai = caminho.parent.name

        match_lote = PADRAO_ARQUIVO_LOTE.match(nome)
        match_sufixo = PADRAO_ID_SUFIXO.search(nome)

        if not match_sufixo:
            erros.append(f"arquivo fora da convencao de nome (sem id_consulta): {caminho_rel}")
            continue

        id_consulta = match_sufixo.group("id_consulta")
        if id_consulta not in registros:
            erros.append(
                f"arquivo orfao (id_consulta {id_consulta} ausente no registro): {caminho_rel}"
            )
            continue

        reg = registros[id_consulta]
        tema_reg = reg["tema"]
        comp_ini = reg["competencia_inicio"]
        comp_fim = reg["competencia_fim"]

        competencia_fmt = ""
        tema_arquivo = tema_reg

        if match_lote:
            tema_arquivo = match_lote.group("tema")
            comp_raw = match_lote.group("competencia")
            competencia_fmt = f"{comp_raw[:4]}-{comp_raw[4:6]}"

            if tema_arquivo != tema_reg:
                erros.append(
                    f"tema divergente no nome do arquivo ({tema_arquivo} != {tema_reg}): {caminho_rel}"
                )
            if pasta_pai != tema_reg:
                avisos.append(
                    f"subpasta '{pasta_pai}' difere do tema '{tema_reg}' em {caminho_rel}"
                )
            if not (comp_ini <= competencia_fmt <= comp_fim):
                erros.append(
                    f"competencia {competencia_fmt} fora da janela [{comp_ini}, {comp_fim}]: {caminho_rel}"
                )
        else:
            # aceita arquivos de consulta unica (ex. coleta manual) desde que o id bata com o registro
            competencia_fmt = f"{comp_ini}..{comp_fim}"

        inspecao = inspecionar_csv_bronze(caminho)
        digest = sha256_arquivo(caminho)

        if inspecao["erro_estrutura"]:
            erros.append(f"falha estrutural em {caminho_rel}: {inspecao['erro_estrutura']}")

        item = {
            "id_consulta": id_consulta,
            "pessoa": reg["pessoa"],
            "tema": tema_reg,
            "competencia": competencia_fmt,
            "nome_arquivo": nome,
            "caminho_relativo": caminho_rel,
            "tamanho_bytes": inspecao["tamanho_bytes"],
            "n_linhas_dados": inspecao["n_linhas_dados"],
            "sha256": digest,
            "status_estrutura": "ok" if not inspecao["erro_estrutura"] else "erro",
        }
        arquivos_por_lote[id_consulta].append(item)
        manifesto_linhas.append(item)

    # valida cobertura por lote registrado e cruza com checkpoint local quando disponivel
    resumo_checkpoint = carregar_resumo_checkpoint(checkpoint_path)
    resumo_lotes: list[dict[str, object]] = []

    for id_consulta, reg in sorted(registros.items()):
        itens = arquivos_por_lote.get(id_consulta, [])
        tema = reg["tema"]
        if not itens:
            erros.append(f"registro sem arquivo correspondente na bronze: {id_consulta} ({tema})")

        # verifica competencias duplicadas dentro do mesmo lote
        comps = [str(i["competencia"]) for i in itens]
        duplicadas = sorted({c for c in comps if comps.count(c) > 1})
        if duplicadas:
            erros.append(
                f"competencias duplicadas no lote {id_consulta} ({tema}): {', '.join(duplicadas)}"
            )

        janela_esperada = gerar_competencias_esperadas(
            reg["competencia_inicio"], reg["competencia_fim"]
        )
        total_linhas_lote = sum(int(i["n_linhas_dados"]) for i in itens)
        total_bytes_lote = sum(int(i["tamanho_bytes"]) for i in itens)
        hash_lote = sha256_lote(
            [(str(i["caminho_relativo"]), str(i["sha256"])) for i in itens]
        )

        qtd_no_data_ckpt = None
        qtd_erro_ckpt = None
        if resumo_checkpoint is not None and tema in resumo_checkpoint:
            ckpt_mod = resumo_checkpoint[tema]
            qtd_down_ckpt = ckpt_mod.get("downloaded", 0)
            qtd_no_data_ckpt = ckpt_mod.get("no_data", 0)
            qtd_erro_ckpt = ckpt_mod.get("error", 0)

            if qtd_down_ckpt != len(itens):
                erros.append(
                    f"divergencia checkpoint x bronze no modulo {tema} ({id_consulta}): "
                    f"checkpoint={qtd_down_ckpt}, disco={len(itens)}"
                )
            if qtd_erro_ckpt > 0:
                erros.append(
                    f"checkpoint indica {qtd_erro_ckpt} erro(s) pendente(s) no modulo {tema}"
                )
            if len(itens) + qtd_no_data_ckpt != len(janela_esperada):
                avisos.append(
                    f"modulo {tema} ({id_consulta}): disco ({len(itens)}) + no_data ({qtd_no_data_ckpt}) "
                    f"!= competencias da janela ({len(janela_esperada)})"
                )

        resumo_lotes.append(
            {
                "id_consulta": id_consulta,
                "pessoa": reg["pessoa"],
                "tema": tema,
                "nome_arquivo_agregado": reg["nome_arquivo_agregado"],
                "competencia_inicio": reg["competencia_inicio"],
                "competencia_fim": reg["competencia_fim"],
                "competencias_janela": len(janela_esperada),
                "arquivos_bronze": len(itens),
                "no_data_checkpoint": qtd_no_data_ckpt,
                "erros_checkpoint": qtd_erro_ckpt,
                "n_linhas_dados_total": total_linhas_lote,
                "tamanho_bytes_total": total_bytes_lote,
                "sha256_lote": hash_lote,
            }
        )

    if salvar_saidas:
        manifesto_csv_path.parent.mkdir(parents=True, exist_ok=True)
        colunas_manifesto = [
            "id_consulta",
            "pessoa",
            "tema",
            "competencia",
            "nome_arquivo",
            "caminho_relativo",
            "tamanho_bytes",
            "n_linhas_dados",
            "sha256",
            "status_estrutura",
        ]
        with manifesto_csv_path.open("w", encoding="utf-8", newline="") as arquivo:
            escritor = csv.DictWriter(arquivo, fieldnames=colunas_manifesto)
            escritor.writeheader()
            escritor.writerows(manifesto_linhas)

        relatorio = {
            "etapa": "E1_conferir_bronze",
            "gerado_em_utc": datetime.now(timezone.utc).isoformat(),
            "status": "aprovado" if not erros else "reprovado",
            "totais": {
                "lotes_registrados": len(registros),
                "arquivos_bronze": len(manifesto_linhas),
                "linhas_dados_somadas": sum(int(i["n_linhas_dados"]) for i in manifesto_linhas),
                "erros": len(erros),
                "avisos": len(avisos),
            },
            "lotes": resumo_lotes,
            "erros": erros,
            "avisos": avisos,
        }
        with relatorio_json_path.open("w", encoding="utf-8") as arquivo:
            json.dump(relatorio, arquivo, ensure_ascii=False, indent=2)

    # imprime resumo legivel no terminal
    print("conferencia e1 (bronze x registro):")
    print(f"  lotes no registro : {len(registros)}")
    print(f"  arquivos na bronze: {len(manifesto_linhas)}")
    print(f"  linhas de dados   : {sum(int(i['n_linhas_dados']) for i in manifesto_linhas)}")
    print()
    print("resumo por lote:")
    for lote in resumo_lotes:
        no_data_txt = (
            f" | no_data={lote['no_data_checkpoint']}"
            if lote["no_data_checkpoint"] is not None
            else ""
        )
        print(
            f"  {lote['id_consulta']} ({lote['tema']:<4}) | "
            f"csvs={lote['arquivos_bronze']:>3}/{lote['competencias_janela']}{no_data_txt} | "
            f"linhas={lote['n_linhas_dados_total']:>6} | "
            f"sha256_lote={str(lote['sha256_lote'])[:12]}..."
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

    if salvar_saidas:
        print(f"\nmanifesto salvo em: {formatar_caminho(manifesto_csv_path, BASE_DIR)}")
        print(f"relatorio salvo em: {formatar_caminho(relatorio_json_path, BASE_DIR)}")
    print("status e1: aprovado (100% conciliado)")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="etapa e1: confere arquivos da bronze contra registro_bronze.csv e gera sha-256."
    )
    parser.add_argument("--bronze-dir", type=Path, default=BRONZE_DIR)
    parser.add_argument("--registro", type=Path, default=REGISTRO_PATH)
    parser.add_argument("--checkpoint", type=Path, default=CHECKPOINT_PATH)
    parser.add_argument("--saida-manifesto", type=Path, default=MANIFESTO_CSV_PATH)
    parser.add_argument("--saida-relatorio", type=Path, default=RELATORIO_JSON_PATH)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="executa a conferencia sem gravar arquivos de manifesto e relatorio",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return conferir_bronze(
        bronze_dir=args.bronze_dir,
        registro_path=args.registro,
        checkpoint_path=args.checkpoint,
        manifesto_csv_path=args.saida_manifesto,
        relatorio_json_path=args.saida_relatorio,
        salvar_saidas=not args.dry_run,
    )


if __name__ == "__main__":
    sys.exit(main())