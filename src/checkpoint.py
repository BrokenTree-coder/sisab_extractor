import sqlite3
from datetime import datetime, timezone
from pathlib import Path


class CheckpointStore:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._inicializar()

    def _conectar(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _inicializar(self) -> None:
        with self._conectar() as conn:
            # ativa modo wal para evitar corrupcao em interrupcoes abruptas
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS extracoes (
                    modulo TEXT NOT NULL,
                    competencia TEXT NOT NULL,
                    status TEXT NOT NULL,
                    arquivo_path TEXT,
                    total_registros INTEGER DEFAULT 0,
                    data_geracao TEXT,
                    tentativas INTEGER DEFAULT 0,
                    ultimo_erro TEXT,
                    atualizado_em TEXT NOT NULL,
                    PRIMARY KEY (modulo, competencia)
                );
                """
            )

    @staticmethod
    def _agora_iso() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def ja_concluido(self, modulo: str, competencia: str) -> bool:
        with self._conectar() as conn:
            row = conn.execute(
                """
                SELECT status, arquivo_path
                FROM extracoes
                WHERE modulo = ? AND competencia = ?
                """,
                (modulo, competencia),
            ).fetchone()

        if row is None:
            return False
        if row["status"] == "no_data":
            return True
        if row["status"] == "downloaded" and row["arquivo_path"]:
            return Path(row["arquivo_path"]).exists()
        return False

    def registrar_sucesso(
        self,
        modulo: str,
        competencia: str,
        arquivo_path: Path,
        total_registros: int,
        data_geracao: str | None,
    ) -> None:
        with self._conectar() as conn:
            conn.execute(
                """
                INSERT INTO extracoes (
                    modulo, competencia, status, arquivo_path,
                    total_registros, data_geracao, tentativas, ultimo_erro, atualizado_em
                )
                VALUES (?, ?, 'downloaded', ?, ?, ?, 1, NULL, ?)
                ON CONFLICT(modulo, competencia) DO UPDATE SET
                    status = 'downloaded',
                    arquivo_path = excluded.arquivo_path,
                    total_registros = excluded.total_registros,
                    data_geracao = excluded.data_geracao,
                    tentativas = extracoes.tentativas + 1,
                    ultimo_erro = NULL,
                    atualizado_em = excluded.atualizado_em
                """,
                (
                    modulo,
                    competencia,
                    str(arquivo_path),
                    total_registros,
                    data_geracao,
                    self._agora_iso(),
                ),
            )

    def registrar_sem_dados(self, modulo: str, competencia: str) -> None:
        with self._conectar() as conn:
            conn.execute(
                """
                INSERT INTO extracoes (
                    modulo, competencia, status, arquivo_path,
                    total_registros, data_geracao, tentativas, ultimo_erro, atualizado_em
                )
                VALUES (?, ?, 'no_data', NULL, 0, NULL, 1, NULL, ?)
                ON CONFLICT(modulo, competencia) DO UPDATE SET
                    status = 'no_data',
                    arquivo_path = NULL,
                    total_registros = 0,
                    data_geracao = NULL,
                    tentativas = extracoes.tentativas + 1,
                    ultimo_erro = NULL,
                    atualizado_em = excluded.atualizado_em
                """,
                (modulo, competencia, self._agora_iso()),
            )

    def registrar_erro(self, modulo: str, competencia: str, mensagem: str) -> None:
        with self._conectar() as conn:
            conn.execute(
                """
                INSERT INTO extracoes (
                    modulo, competencia, status, arquivo_path,
                    total_registros, data_geracao, tentativas, ultimo_erro, atualizado_em
                )
                VALUES (?, ?, 'error', NULL, 0, NULL, 1, ?, ?)
                ON CONFLICT(modulo, competencia) DO UPDATE SET
                    status = 'error',
                    tentativas = extracoes.tentativas + 1,
                    ultimo_erro = excluded.ultimo_erro,
                    atualizado_em = excluded.atualizado_em
                """,
                (modulo, competencia, mensagem[:500], self._agora_iso()),
            )

    def obter_ou_criar_lote(self, modulo: str) -> str:
        # reaproveita o codigo Jnnnn ja aberto para o modulo, ou cria o proximo da sequencia global
        with self._conectar() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS lotes_consulta (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    codigo_j TEXT NOT NULL UNIQUE,
                    modulo TEXT NOT NULL,
                    criado_em TEXT NOT NULL
                );
                """
            )
            existente = conn.execute(
                "SELECT codigo_j FROM lotes_consulta WHERE modulo = ? ORDER BY seq DESC LIMIT 1",
                (modulo,),
            ).fetchone()
            if existente:
                return existente["codigo_j"]

            ultimo = conn.execute(
                "SELECT seq FROM lotes_consulta ORDER BY seq DESC LIMIT 1"
            ).fetchone()
            prox_seq = (ultimo["seq"] + 1) if ultimo else 1
            codigo_j = f"J{prox_seq:04d}"

            conn.execute(
                "INSERT INTO lotes_consulta (codigo_j, modulo, criado_em) VALUES (?, ?, ?)",
                (codigo_j, modulo, self._agora_iso()),
            )
            return codigo_j

    def resumo(self, modulo: str | None = None) -> list[dict]:
        query = """
            SELECT modulo, status, COUNT(*) AS qtd
            FROM extracoes
        """
        params: tuple = ()
        if modulo:
            query += " WHERE modulo = ?"
            params = (modulo,)
        query += " GROUP BY modulo, status ORDER BY modulo, status"

        with self._conectar() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]