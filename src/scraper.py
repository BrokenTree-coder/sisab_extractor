import random
import re
import time
from dataclasses import dataclass
from pathlib import Path
from playwright.sync_api import (
    Page,
    TimeoutError as PlaywrightTimeoutError,
    expect,
    sync_playwright,
)

from src.auditor import auditar_csv_sisab, ym_para_sisab
from src.checkpoint import CheckpointStore


@dataclass
class ConfigExecucao:
    headless: bool = True
    timeout_download_ms: int = 60000
    timeout_navegacao_ms: int = 90000
    pausa_min_s: float = 2.5
    pausa_max_s: float = 5.0
    renovar_sessao_a_cada: int = 40
    tentativas_por_competencia: int = 3
    backoff_segundos: tuple[int, ...] = (15, 45, 120)
    max_falhas_consecutivas: int = 3


class ErroServidorSisab(RuntimeError):
    pass


class ErroAuditoriaCsv(RuntimeError):
    pass

class _RespostaSemDados(Exception):
    pass

class SisabScraper:
    def __init__(
        self,
        portal_cfg: dict,
        pasta_bronze: Path,
        checkpoint: CheckpointStore,
        exec_cfg: ConfigExecucao | None = None,
    ):
        self.portal = portal_cfg
        self.pasta_bronze = pasta_bronze
        self.checkpoint = checkpoint
        self.cfg = exec_cfg or ConfigExecucao()

    def _selecionar_opcao_flexivel(self, page: Page, seletor: str, alvo: str) -> None:
        # localiza o valor real da option comparando por value ou rotulo visivel
        valor_encontrado = page.locator(seletor).evaluate(
            """
            (select, target) => {
                const norm = target.trim().toLowerCase();
                for (const opt of select.options) {
                    if (opt.value.trim().toLowerCase() === norm) return opt.value;
                    if (opt.text.trim().toLowerCase() === norm) return opt.value;
                }
                return null;
            }
            """,
            alvo,
        )
        if valor_encontrado is None:
            raise RuntimeError(f"opcao '{alvo}' nao encontrada em {seletor}")
        page.locator(seletor).select_option(valor_encontrado)

    def _verificar_pagina_integra(self, page: Page, aguardar_ms: int = 90000) -> None:
        # aguarda o formulario estabilizar caso um postback jsf esteja recarregando a pagina
        try:
            page.wait_for_load_state("domcontentloaded", timeout=aguardar_ms)
            expect(page.locator("#selectLinha")).to_be_visible(timeout=aguardar_ms)
        except Exception:
            pass

        # detecta paginas de erro do jboss ou perda real do formulario jsf
        conteudo = page.content()
        marcadores_erro = (
            "JBWEB000065",
            "HTTP Status 500",
            "HTTP Status 502",
            "HTTP Status 503",
            "HTTP Status 504",
            "HTTP Status 403",
            "Exception report",
        )
        for marcador in marcadores_erro:
            if marcador in conteudo:
                raise ErroServidorSisab(f"erro de servidor detectado na pagina: {marcador}")

        if page.locator("#selectLinha").count() == 0:
            titulo = ""
            try:
                titulo = page.title()
            except Exception:
                pass
            raise ErroServidorSisab(
                f"formulario principal indisponivel (url={page.url}, titulo='{titulo}')"
            )


    def _sessao_preservada(self, page: Page, codigos: list[dict[str, str]]) -> bool:
        # confere se o postback de mes vazio manteve os eixos e os cids marcados
        try:
            linha_ok = page.locator("#selectLinha").input_value() == self.portal["linha"]
            cids_marcados = page.locator("#modal-default-cid input[type='checkbox']:checked").count()
            return linha_ok and cids_marcados == len(codigos)
        except Exception:
            return False
    
    def _preparar_sessao(self, page: Page, codigos: list[dict[str, str]]) -> None:
        page.goto(
            self.portal["url"],
            wait_until="domcontentloaded",
            timeout=self.cfg.timeout_navegacao_ms,
        )
        expect(page.locator("#selectLinha")).to_be_visible(timeout=30000)
        self._verificar_pagina_integra(page)

        self._selecionar_opcao_flexivel(page, "#selectLinha", self.portal["linha"])
        self._selecionar_opcao_flexivel(page, "#selectcoluna", self.portal["coluna"])
        self._selecionar_opcao_flexivel(page, "#unidGeo", self.portal["unidade_geografica"])

        # aguarda eventual atualizacao ajax do painel apos definir a unidade geografica
        expect(page.locator("#competencia button.multiselect")).to_be_visible(timeout=15000)

        # abre o modal e marca estritamente cada codigo especificado no catalogo
        page.get_by_role("button", name="CIAP/CID").click()
        modal = page.locator("#modal-default-cid")
        expect(modal).to_be_visible(timeout=15000)

        busca = modal.get_by_role("searchbox", name="Procurar:")
        for item in codigos:
            sistema = item["sistema"].upper()
            codigo = item["codigo"].upper()

            busca.fill("")
            busca.fill(codigo.lower())

            padrao = re.compile(
                rf"^{re.escape(sistema)} \({re.escape(codigo)}\)",
                re.IGNORECASE,
            )
            alvo = modal.get_by_role("checkbox", name=padrao)
            expect(alvo.first).to_be_visible(timeout=10000)

            qtd = alvo.count()
            if qtd != 1:
                raise RuntimeError(
                    f"esperado exatamente um checkbox para {sistema} ({codigo}), encontrados: {qtd}"
                )
            if not alvo.first.is_checked():
                alvo.first.check()

        modal.locator("button", has_text="Concluir").click()
        expect(modal).to_be_hidden(timeout=15000)

    def _selecionar_competencia_unica(self, page: Page, rotulo_sisab: str) -> None:
        botao_comp = page.locator("#competencia button.multiselect")
        botao_comp.click()

        # desmarca qualquer competencia anterior ainda ativa no multiselect
        marcados = page.locator("#competencia input[type='checkbox']:checked")
        while marcados.count() > 0:
            marcados.first.uncheck()

        alvo = page.locator("#competencia").get_by_role("checkbox", name=rotulo_sisab)
        if alvo.count() == 0:
            botao_comp.click()
            raise RuntimeError(f"competencia '{rotulo_sisab}' nao encontrada no portal")

        alvo.first.check()
        botao_comp.click()

    def _tentar_extrair_competencia(
        self,
        page: Page,
        modulo: str,
        competencia_ym: str,
        codigo_j: str,
        codigos: list[dict[str, str]],
    ) -> str:
        rotulo_sisab = ym_para_sisab(competencia_ym)
        self._selecionar_competencia_unica(page, rotulo_sisab)

        pasta_modulo = self.pasta_bronze / modulo
        pasta_modulo.mkdir(parents=True, exist_ok=True)

        comp_compacta = competencia_ym.replace("-", "")
        arquivo_final = pasta_modulo / f"SISAB_{modulo}_{comp_compacta}_{codigo_j}.csv"
        arquivo_tmp = pasta_modulo / f"SISAB_{modulo}_{comp_compacta}_{codigo_j}.csv.tmp"

        if arquivo_tmp.exists():
            arquivo_tmp.unlink()

        resposta_vazia_detectada = False
        download_iniciado = False

        def monitorar_resposta(resp) -> None:
            nonlocal resposta_vazia_detectada
            # intercepta o post de exportacao quando o servidor responde 204 (sem conteudo)
            if (
                "RelSauProducao" in resp.url
                and resp.request.method == "POST"
                and resp.status == 204
            ):
                resposta_vazia_detectada = True

        def monitorar_download(_download) -> None:
            nonlocal download_iniciado
            # libera o loop imediatamente assim que o download real comeca
            download_iniciado = True

        page.on("response", monitorar_resposta)
        page.on("download", monitorar_download)
        page.get_by_role("button", name="Download").click()

        try:
            with page.expect_download(timeout=self.cfg.timeout_download_ms) as download_info:
                page.get_by_role("link", name="Csv").click()

                limite_ms = self.cfg.timeout_download_ms
                intervalo_ms = 200
                decorrido_ms = 0
                while (
                    not resposta_vazia_detectada
                    and not download_iniciado
                    and decorrido_ms < limite_ms
                ):
                    page.wait_for_timeout(intervalo_ms)
                    decorrido_ms += intervalo_ms

                if resposta_vazia_detectada:
                    raise _RespostaSemDados()

        except _RespostaSemDados:
            self._verificar_pagina_integra(page)
            page.keyboard.press("Escape")
            self.checkpoint.registrar_sem_dados(modulo, competencia_ym)
            return "no_data"

        except PlaywrightTimeoutError:
            # sem 204 e sem download dentro do limite: trata como erro de rede para retentativa
            raise RuntimeError(
                f"timeout ({self.cfg.timeout_download_ms} ms) aguardando inicio do download csv"
            )
        finally:
            page.remove_listener("response", monitorar_resposta)
            page.remove_listener("download", monitorar_download)

        download = download_info.value
        download.save_as(arquivo_tmp)

        # valida cabecalho, competencia, filtros e rodape antes de promover o arquivo
        auditoria = auditar_csv_sisab(arquivo_tmp, competencia_ym, codigos)
        if not auditoria.valido:
            arquivo_tmp.unlink(missing_ok=True)
            raise ErroAuditoriaCsv("; ".join(auditoria.erros))

        arquivo_tmp.replace(arquivo_final)
        self.checkpoint.registrar_sucesso(
            modulo=modulo,
            competencia=competencia_ym,
            arquivo_path=arquivo_final,
            total_registros=auditoria.total_registros,
            data_geracao=auditoria.data_geracao,
        )
        return f"downloaded ({auditoria.total_registros} registros)"

    def executar_modulo(
        self,
        modulo: str,
        codigos: list[dict[str, str]],
        competencias: list[str],
        limite: int | None = None,
    ) -> None:
        pendentes = [c for c in competencias if not self.checkpoint.ja_concluido(modulo, c)]
        if limite is not None:
            pendentes = pendentes[:limite]

        if not pendentes:
            print(f"[{modulo}] nenhuma competencia pendente.")
            return

        # obtem ou cria um unico codigo sequencial de lote para todas as competencias do modulo
        codigo_j = self.checkpoint.obter_ou_criar_lote(modulo)

        print(
            f"[{modulo}] iniciando extracao de {len(pendentes)} competencia(s) "
            f"(lote {codigo_j}, total na janela: {len(competencias)})."
        )

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.cfg.headless)
            context = None
            page = None

            sessao_pronta = False
            requisicoes_na_sessao = 0
            falhas_consecutivas = 0

            try:
                for idx, comp in enumerate(pendentes, start=1):
                    # recarrega a sessao jsf periodicamente para evitar expiracao de viewstate
                    if sessao_pronta and requisicoes_na_sessao >= self.cfg.renovar_sessao_a_cada:
                        print(f"[{modulo}] renovando sessao jsf preventiva...")
                        sessao_pronta = False
                        requisicoes_na_sessao = 0

                    concluido = False
                    ultimo_erro = ""

                    for tentativa in range(1, self.cfg.tentativas_por_competencia + 1):
                        try:
                            if not sessao_pronta:
                                # abre um novo contexto para obter um jsessionid limpo no servidor
                                if context is not None:
                                    try:
                                        context.close()
                                    except Exception:
                                        pass
                                context = browser.new_context(accept_downloads=True)
                                page = context.new_page()
                                self._preparar_sessao(page, codigos)
                                sessao_pronta = True
                                requisicoes_na_sessao = 0

                            assert page is not None
                            inicio = time.perf_counter()
                            desfecho = self._tentar_extrair_competencia(
                                page, modulo, comp, codigo_j, codigos
                            )
                            duracao = time.perf_counter() - inicio

                            requisicoes_na_sessao += 1
                            falhas_consecutivas = 0
                            concluido = True

                            # se o reload de um mes vazio limpou o formulario, reconfigura na proxima iteracao
                            if desfecho == "no_data" and not self._sessao_preservada(page, codigos):
                                sessao_pronta = False
                                requisicoes_na_sessao = 0

                            print(
                                f"[{modulo}] ({idx}/{len(pendentes)}) {comp}: "
                                f"{desfecho} em {duracao:.2f}s"
                            )
                            break

                        except Exception as exc:
                            ultimo_erro = f"{type(exc).__name__}: {exc}"
                            print(
                                f"[{modulo}] {comp} falhou na tentativa "
                                f"{tentativa}/{self.cfg.tentativas_por_competencia}: {ultimo_erro}"
                            )
                            # descarta a sessao atual para garantir estado limpo na proxima tentativa
                            sessao_pronta = False
                            requisicoes_na_sessao = 0

                            if tentativa < self.cfg.tentativas_por_competencia:
                                espera = self.cfg.backoff_segundos[
                                    min(tentativa - 1, len(self.cfg.backoff_segundos) - 1)
                                ]
                                print(f"[{modulo}] aguardando {espera}s antes de tentar novamente...")
                                time.sleep(espera)

                    if not concluido:
                        self.checkpoint.registrar_erro(modulo, comp, ultimo_erro)
                        falhas_consecutivas += 1
                        if falhas_consecutivas >= self.cfg.max_falhas_consecutivas:
                            print(
                                f"[{modulo}] disjuntor acionado apos {falhas_consecutivas} "
                                "competencias com erro consecutivo. encerrando para proteger o portal."
                            )
                            break

                    # aplica pausa aleatoria entre consultas para manter taxa moderada
                    if idx < len(pendentes):
                        pausa = random.uniform(self.cfg.pausa_min_s, self.cfg.pausa_max_s)
                        time.sleep(pausa)

            finally:
                # evita excecao secundaria ao interromper a execucao com ctrl+c
                for recurso in (context, browser):
                    if recurso is not None:
                        try:
                            recurso.close()
                        except Exception:
                            pass