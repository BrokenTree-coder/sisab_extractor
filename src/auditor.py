import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path


# mapeia mes numerico para sigla usada pelo portal do sisab
MESES_SISAB = {
    1: "JAN",
    2: "FEV",
    3: "MAR",
    4: "ABR",
    5: "MAI",
    6: "JUN",
    7: "JUL",
    8: "AGO",
    9: "SET",
    10: "OUT",
    11: "NOV",
    12: "DEZ",
}

PADRAO_COMPETENCIA = re.compile(r"\b[A-Z]{3}/\d{4}\b")
PADRAO_CODIGO = re.compile(r"\b(CID|CIAP)\s*\(([A-Z0-9]+)\)")
PADRAO_IBGE = re.compile(r"^\d{6,7}$")


@dataclass
class ResultadoAuditoria:
    valido: bool
    erros: list[str] = field(default_factory=list)
    competencias_encontradas: list[str] = field(default_factory=list)
    codigos_no_filtro: set[str] = field(default_factory=set)
    codigos_nas_colunas: list[str] = field(default_factory=list)
    total_registros: int = 0
    data_geracao: str | None = None


def ym_para_sisab(competencia_ym: str) -> str:
    # converte competencia do padrao aaaa-mm para o rotulo do sisab
    ano_str, mes_str = competencia_ym.split("-")
    mes = int(mes_str)
    if mes not in MESES_SISAB:
        raise ValueError(f"mes invalido na competencia: {competencia_ym}")
    return f"{MESES_SISAB[mes]}/{ano_str}"


def _extrair_codigos(texto: str) -> list[str]:
    # extrai pares de sistema e codigo a partir do texto
    encontrados = PADRAO_CODIGO.findall(texto.upper())
    return [f"{sistema}:{codigo}" for sistema, codigo in encontrados]


def _dividir_linha_csv(linha: str) -> list[str]:
    # divide linha por ponto e virgula descartando campo vazio final
    campos = [campo.strip() for campo in linha.strip().split(";")]
    if campos and campos[-1] == "":
        campos.pop()
    return campos


def auditar_csv_sisab(
    caminho_csv: Path,
    competencia_ym: str,
    codigos_esperados: list[dict[str, str]],
) -> ResultadoAuditoria:
    erros: list[str] = []
    competencia_esperada = ym_para_sisab(competencia_ym)
    esperados_set = {
        f"{item['sistema'].upper()}:{item['codigo'].upper()}"
        for item in codigos_esperados
    }

    if not caminho_csv.exists() or caminho_csv.stat().st_size == 0:
        return ResultadoAuditoria(valido=False, erros=["arquivo inexistente ou vazio"])

    # le o arquivo bruto preservando a codificacao padrao do sisab
    linhas = caminho_csv.read_text(encoding="latin-1", errors="replace").splitlines()

    competencias_encontradas: list[str] = []
    codigos_no_filtro: set[str] = set()
    indice_cabecalho: int | None = None
    colunas_cabecalho: list[str] = []

    # percorre o bloco de metadados ate localizar o cabecalho da tabela
    for idx, linha_bruta in enumerate(linhas):
        linha_limpa = linha_bruta.strip().strip('"')
        linha_lower = linha_limpa.lower()

        if linha_lower.startswith("competência:") or linha_lower.startswith("competencia:"):
            competencias_encontradas = PADRAO_COMPETENCIA.findall(linha_limpa.upper())
        elif linha_lower.startswith("ciap/cid:"):
            codigos_no_filtro.update(_extrair_codigos(linha_limpa))
        elif linha_lower.startswith("uf;ibge;municipio;"):
            indice_cabecalho = idx
            colunas_cabecalho = _dividir_linha_csv(linha_bruta)
            break

    # garante que exatamente a competencia solicitada foi aplicada no filtro
    if competencias_encontradas != [competencia_esperada]:
        erros.append(
            f"competencia divergente: esperado [{competencia_esperada}], "
            f"encontrado {competencias_encontradas}"
        )

    # confere se os codigos aplicados no filtro batem exatamente com o catalogo
    if codigos_no_filtro != esperados_set:
        erros.append(
            f"filtro cid divergente: esperado {sorted(esperados_set)}, "
            f"encontrado {sorted(codigos_no_filtro)}"
        )

    if indice_cabecalho is None:
        erros.append("cabecalho da tabela (Uf;Ibge;Municipio;) nao encontrado")
        return ResultadoAuditoria(
            valido=False,
            erros=erros,
            competencias_encontradas=competencias_encontradas,
            codigos_no_filtro=codigos_no_filtro,
        )

    # valida as colunas de cid presentes no cabecalho truncado da tabela
    codigos_nas_colunas: list[str] = []
    for col in colunas_cabecalho[3:]:
        extraidos = _extrair_codigos(col)
        if not extraidos:
            erros.append(f"coluna de codigo nao reconhecida no cabecalho: {col}")
        else:
            codigos_nas_colunas.append(extraidos[0])

    colunas_fora_do_catalogo = set(codigos_nas_colunas) - esperados_set
    if colunas_fora_do_catalogo:
        erros.append(f"colunas fora do catalogo esperado: {sorted(colunas_fora_do_catalogo)}")

    # le as linhas de dados ate encontrar o rodape de auditoria
    total_registros = 0
    encontrou_rodape = False
    data_geracao: str | None = None
    num_colunas = len(colunas_cabecalho)

    for linha_bruta in linhas[indice_cabecalho + 1 :]:
        texto = linha_bruta.strip()
        if not texto:
            continue

        texto_lower = texto.lower()
        if texto_lower.startswith("fonte:"):
            encontrou_rodape = True
            continue
        if texto_lower.startswith("dado gerado em:"):
            data_geracao = texto.split(":", 1)[1].strip()
            continue

        if encontrou_rodape:
            continue

        campos = _dividir_linha_csv(linha_bruta)
        if len(campos) != num_colunas:
            erros.append(f"linha com quantidade inesperada de colunas: {texto}")
            continue
        if not PADRAO_IBGE.match(campos[1]):
            erros.append(f"codigo ibge invalido na linha: {texto}")
            continue

        total_registros += 1

    # confirma que o arquivo chegou ate o rodape sem truncamento
    if not encontrou_rodape:
        erros.append("rodape oficial (Fonte:) ausente; possivel arquivo truncado")

    return ResultadoAuditoria(
        valido=len(erros) == 0,
        erros=erros,
        competencias_encontradas=competencias_encontradas,
        codigos_no_filtro=codigos_no_filtro,
        codigos_nas_colunas=codigos_nas_colunas,
        total_registros=total_registros,
        data_geracao=data_geracao,
    )


if __name__ == "__main__":
    # utilitario de linha de comando para validar um csv ja baixado
    if len(sys.argv) != 4:
        print("uso: python src/auditor.py <caminho_csv> <modulo> <competencia_aaaa-mm>")
        sys.exit(1)

    caminho_arquivo = Path(sys.argv[1])
    modulo = sys.argv[2]
    competencia = sys.argv[3]

    catalogo_path = Path(__file__).resolve().parent.parent / "config" / "catalogo_morbidades.json"
    catalogo = json.loads(catalogo_path.read_text(encoding="utf-8"))

    if modulo not in catalogo["modulos"]:
        print(f"modulo desconhecido: {modulo}")
        sys.exit(1)

    codigos = catalogo["modulos"][modulo]["codigos"]
    resultado = auditar_csv_sisab(caminho_arquivo, competencia, codigos)

    print(f"valido: {resultado.valido}")
    print(f"competencias: {resultado.competencias_encontradas}")
    print(f"filtro cid: {sorted(resultado.codigos_no_filtro)}")
    print(f"colunas tabela: {resultado.codigos_nas_colunas}")
    print(f"registros: {resultado.total_registros}")
    print(f"data geracao: {resultado.data_geracao}")
    if resultado.erros:
        print("erros:", resultado.erros)