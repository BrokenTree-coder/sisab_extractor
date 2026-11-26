from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from etl.e1_conferir_bronze import (
    BRONZE_DIR,
    CHECKPOINT_PATH,
    MANIFESTO_CSV_PATH,
    REGISTRO_PATH,
    RELATORIO_JSON_PATH,
    conferir_bronze,
)
from etl.e2_padronizar import (
    RELATORIO_E2_PATH,
    SILVER_CSV_PATH,
    padronizar_bronze_para_silver,
)
from etl.e3_agregar import (
    GOLD_DIR,
    GOLD_MUN_CSV_PATH,
    GOLD_UF_CSV_PATH,
    RELATORIO_E3_PATH,
    agregar_silver_para_gold,
)
from etl.e4_validar import (
    COBERTURA_V5_CSV_PATH,
    COMPARACAO_V6_CSV_PATH,
    MUNICIPIOS_REF_PATH,
    RELATORIO_E4_PATH,
    executar_validacoes,
)
from etl.e5_relatar import (
    MAPA_COBERTURA_CSV_PATH,
    MAPA_COBERTURA_MD_PATH,
    executar_relatorio_e5,
    formatar_caminho,
    gravar,
)

ETAPAS_ORDEM = ["E1", "E2", "E3", "E4", "E5"]


def normalizar_etapas(etapas: list[str] | None) -> list[str]:
    # resolve lista de etapas em ordem canonica sem duplicatas
    if not etapas:
        return list(ETAPAS_ORDEM)
    selecionadas = {e.strip().upper() for e in etapas if e.strip()}
    desconhecidas = sorted(selecionadas - set(ETAPAS_ORDEM))
    if desconhecidas:
        raise ValueError(f"etapa(s) desconhecida(s): {', '.join(desconhecidas)}")
    return [e for e in ETAPAS_ORDEM if e in selecionadas]


def rodar_pipeline(args: argparse.Namespace) -> int:
    # executa sequencialmente as etapas selecionadas e consolida o manifesto final
    inicio_utc = datetime.now(timezone.utc)
    t0 = time.monotonic()

    try:
        etapas_alvo = normalizar_etapas(args.etapas)
    except ValueError as exc:
        print(f"[erro] {exc}")
        return 1

    resultados_etapas: list[dict[str, object]] = []
    falhas_etapas: list[dict[str, object]] = []
    codigo_saida = 0

    for etapa in etapas_alvo:
        etapa_inicio = time.monotonic()
        print(f"=== [{etapa}] iniciando ===")

        if etapa == "E1":
            rc = conferir_bronze(
                bronze_dir=args.bronze_dir,
                registro_path=args.registro,
                checkpoint_path=args.checkpoint,
                manifesto_csv_path=args.saida_manifesto_e1,
                relatorio_json_path=args.relatorio_e1,
                salvar_saidas=not args.dry_run_e1,
            )
        elif etapa == "E2":
            rc = padronizar_bronze_para_silver(
                bronze_dir=args.bronze_dir,
                registro_path=args.registro,
                silver_csv_path=args.silver_csv,
                relatorio_path=args.relatorio_e2,
                manter_zeros=args.manter_zeros,
                gerar_parquet=args.parquet,
            )
        elif etapa == "E3":
            rc = agregar_silver_para_gold(
                silver_csv_path=args.silver_csv,
                gold_mun_csv_path=args.gold_mun,
                gold_uf_csv_path=args.gold_uf,
                relatorio_path=args.relatorio_e3,
                prefixo_indicador=args.prefixo_indicador,
                gerar_parquet=args.parquet,
            )
        elif etapa == "E4":
            args_e4 = argparse.Namespace(
                bronze_dir=args.bronze_dir,
                registro=args.registro,
                silver_csv=args.silver_csv,
                gold_mun=args.gold_mun,
                gold_uf=args.gold_uf,
                municipios_ref=args.municipios_ref,
                saida_relatorio=args.relatorio_e4,
                saida_cobertura=args.saida_cobertura_v5,
                saida_v6=args.saida_v6,
                baixar_municipios=args.baixar_municipios,
                sem_download_ibge=args.sem_download_ibge,
                base_externa=args.base_externa,
                v6_tema=args.v6_tema,
                v6_col_uf=args.v6_col_uf,
                v6_col_ano=args.v6_col_ano,
                v6_col_valor=args.v6_col_valor,
            )
            rc = executar_validacoes(args_e4)
        elif etapa == "E5":
            rc = executar_relatorio_e5(
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
                execucao_extra={
                    "modo": "rodar_etl_sisab",
                    "etapas_solicitadas": etapas_alvo,
                    "parametros": {
                        "MANTER_ZEROS": args.manter_zeros,
                        "PREFIXO_INDICADOR": args.prefixo_indicador,
                        "GERAR_PARQUET": args.parquet,
                        "BASE_EXTERNA_V6": (
                            formatar_caminho(args.base_externa, BASE_DIR)
                            if args.base_externa
                            else None
                        ),
                    },
                },
            )
        else:
            rc = 1

        duracao_s = round(time.monotonic() - etapa_inicio, 3)
        status_etapa = "aprovado" if rc == 0 else "reprovado"
        resultados_etapas.append(
            {
                "etapa": etapa,
                "status": status_etapa,
                "codigo_retorno": rc,
                "duracao_segundos": duracao_s,
            }
        )
        print(f"=== [{etapa}] {status_etapa} ({duracao_s}s) ===\n")

        if rc != 0:
            codigo_saida = rc
            falhas_etapas.append(
                {
                    "etapa": etapa,
                    "codigo_retorno": rc,
                    "erro": f"etapa {etapa} retornou codigo {rc}",
                }
            )
            if not args.continuar_em_falha:
                break

    # grava manifesto de encerramento quando e5 nao rodou ou quando houve falha
    if "E5" not in etapas_alvo or codigo_saida != 0:
        gravar(
            gold=args.gold_dir,
            bronze=args.bronze_dir,
            raiz=BASE_DIR,
            execucao={
                "modo": "rodar_etl_sisab",
                "inicio_utc": inicio_utc.isoformat(timespec="seconds"),
                "duracao_total_segundos": round(time.monotonic() - t0, 3),
                "etapas_solicitadas": etapas_alvo,
                "status_geral": "aprovado" if codigo_saida == 0 else "reprovado",
                "parametros": {
                    "BRONZE_DIR": formatar_caminho(args.bronze_dir, BASE_DIR),
                    "REGISTRO_PATH": formatar_caminho(args.registro, BASE_DIR),
                    "SILVER_CSV_PATH": formatar_caminho(args.silver_csv, BASE_DIR),
                    "GOLD_MUN_CSV_PATH": formatar_caminho(args.gold_mun, BASE_DIR),
                    "GOLD_UF_CSV_PATH": formatar_caminho(args.gold_uf, BASE_DIR),
                },
            },
            resultados=resultados_etapas,
            falhas=falhas_etapas,
        )

    duracao_total = round(time.monotonic() - t0, 3)
    print(
        f"resumo pipeline sisab: etapas={','.join(etapas_alvo)} | "
        f"duracao={duracao_total}s | "
        f"status={'aprovado' if codigo_saida == 0 else 'reprovado'}"
    )
    return codigo_saida


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="orquestrador do pipeline etl sisab (etapas e1 a e5)."
    )
    parser.add_argument(
        "--etapas",
        nargs="+",
        default=None,
        help="subconjunto de etapas a executar (ex: --etapas E2 E3 E4 E5). Padrao: todas (E1..E5)",
    )
    parser.add_argument(
        "--continuar-em-falha",
        action="store_true",
        help="prossegue para as proximas etapas mesmo se uma etapa anterior reprovar",
    )
    parser.add_argument("--bronze-dir", type=Path, default=BRONZE_DIR)
    parser.add_argument("--gold-dir", type=Path, default=GOLD_DIR)
    parser.add_argument("--registro", type=Path, default=REGISTRO_PATH)
    parser.add_argument("--checkpoint", type=Path, default=CHECKPOINT_PATH)
    parser.add_argument("--municipios-ref", type=Path, default=MUNICIPIOS_REF_PATH)
    parser.add_argument("--silver-csv", type=Path, default=SILVER_CSV_PATH)
    parser.add_argument("--gold-mun", type=Path, default=GOLD_MUN_CSV_PATH)
    parser.add_argument("--gold-uf", type=Path, default=GOLD_UF_CSV_PATH)

    parser.add_argument("--saida-manifesto-e1", type=Path, default=MANIFESTO_CSV_PATH)
    parser.add_argument("--relatorio-e1", type=Path, default=RELATORIO_JSON_PATH)
    parser.add_argument("--relatorio-e2", type=Path, default=RELATORIO_E2_PATH)
    parser.add_argument("--relatorio-e3", type=Path, default=RELATORIO_E3_PATH)
    parser.add_argument("--relatorio-e4", type=Path, default=RELATORIO_E4_PATH)
    parser.add_argument("--saida-cobertura-v5", type=Path, default=COBERTURA_V5_CSV_PATH)
    parser.add_argument("--saida-v6", type=Path, default=COMPARACAO_V6_CSV_PATH)
    parser.add_argument("--saida-mapa-csv", type=Path, default=MAPA_COBERTURA_CSV_PATH)
    parser.add_argument("--saida-mapa-md", type=Path, default=MAPA_COBERTURA_MD_PATH)

    parser.add_argument("--manter-zeros", action="store_true")
    parser.add_argument("--prefixo-indicador", type=str, default="")
    parser.add_argument("--parquet", action="store_true")
    parser.add_argument("--baixar-municipios", action="store_true")
    parser.add_argument("--sem-download-ibge", action="store_true")
    parser.add_argument("--base-externa", type=Path, default=None)
    parser.add_argument("--v6-tema", type=str, default=None)
    parser.add_argument("--v6-col-uf", type=str, default="uf")
    parser.add_argument("--v6-col-ano", type=str, default="ano")
    parser.add_argument("--v6-col-valor", type=str, default="valor")
    parser.add_argument("--dry-run-e1", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return rodar_pipeline(args)


if __name__ == "__main__":
    sys.exit(main())