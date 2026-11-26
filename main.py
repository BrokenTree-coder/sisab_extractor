import argparse
import json
from pathlib import Path

from src.checkpoint import CheckpointStore
from src.scraper import ConfigExecucao, SisabScraper


BASE_DIR = Path(__file__).resolve().parent
CATALOGO_PATH = BASE_DIR / "config" / "catalogo_morbidades.json"
CHECKPOINT_PATH = BASE_DIR / "data" / "checkpoint.db"
BRONZE_DIR = BASE_DIR / "data" / "bronze"


def gerar_competencias(inicio_ym: str, fim_ym: str) -> list[str]:
    ano_ini, mes_ini = map(int, inicio_ym.split("-"))
    ano_fim, mes_fim = map(int, fim_ym.split("-"))

    if (ano_ini, mes_ini) > (ano_fim, mes_fim):
        raise ValueError(f"periodo invalido: inicio {inicio_ym} maior que fim {fim_ym}")

    resultado: list[str] = []
    ano, mes = ano_ini, mes_ini
    while (ano, mes) <= (ano_fim, mes_fim):
        resultado.append(f"{ano:04d}-{mes:02d}")
        mes += 1
        if mes > 12:
            mes = 1
            ano += 1
    return resultado


def exibir_status(checkpoint: CheckpointStore, modulo: str | None) -> None:
    alvo = None if modulo == "todos" else modulo
    linhas = checkpoint.resumo(alvo)
    if not linhas:
        print("nenhum registro encontrado no checkpoint.")
        return

    print("resumo do checkpoint:")
    for item in linhas:
        print(f"  {item['modulo']:<12} | {item['status']:<10} | {item['qtd']}")


def main() -> None:
    catalogo = json.loads(CATALOGO_PATH.read_text(encoding="utf-8"))
    modulos_validos = list(catalogo["modulos"].keys())

    parser = argparse.ArgumentParser(
        description="extrator automatizado do sisab por modulo epidemiologico"
    )
    parser.add_argument(
        "--modulo",
        default="todos",
        choices=modulos_validos + ["todos"],
        help="modulo a executar (ou 'todos')",
    )
    parser.add_argument(
        "--inicio",
        default=catalogo["periodo"]["inicio"],
        help="competencia inicial no formato aaaa-mm",
    )
    parser.add_argument(
        "--fim",
        default=catalogo["periodo"]["fim"],
        help="competencia final no formato aaaa-mm",
    )
    parser.add_argument(
        "--limite",
        type=int,
        default=None,
        help="limita a quantidade de competencias pendentes executadas neste teste",
    )
    parser.add_argument(
        "--timeout-download",
        type=int,
        default=60,
        help="tempo maximo em segundos aguardando inicio do download antes de erro (204 encerra como no_data)",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="abre o navegador visivel para inspecao",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="exibe apenas o resumo atual do checkpoint e encerra",
    )

    args = parser.parse_args()
    checkpoint = CheckpointStore(CHECKPOINT_PATH)

    if args.status:
        exibir_status(checkpoint, args.modulo)
        return

    competencias = gerar_competencias(args.inicio, args.fim)
    modulos_alvo = modulos_validos if args.modulo == "todos" else [args.modulo]

    exec_cfg = ConfigExecucao(
        headless=not args.headed,
        timeout_download_ms=args.timeout_download * 1000,
    )
    scraper = SisabScraper(
        portal_cfg=catalogo["portal"],
        pasta_bronze=BRONZE_DIR,
        checkpoint=checkpoint,
        exec_cfg=exec_cfg,
    )

    for modulo in modulos_alvo:
        codigos = catalogo["modulos"][modulo]["codigos"]
        scraper.executar_modulo(
            modulo=modulo,
            codigos=codigos,
            competencias=competencias,
            limite=args.limite,
        )

    exibir_status(checkpoint, args.modulo)


if __name__ == "__main__":
    main()