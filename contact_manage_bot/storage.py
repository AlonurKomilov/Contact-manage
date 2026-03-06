import asyncio
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .encryption import SessionCipher


@dataclass
class TelegramAccount:
    owner_user_id: int
    phone: str
    telegram_user_id: int
    username: str
    session_string: str


@dataclass
class PendingAuth:
    owner_user_id: int
    phone: str
    phone_code_hash: str
    session_string: str


@dataclass
class SourceConfig:
    owner_user_id: int
    active_source: str
    google_sheet_id: str
    google_worksheet: str
    yandex_csv_url: str
    next_index: int


@dataclass
class OperationStats:
    count: int
    amount: int
    last_event_ts: int | None


class Storage:
    def __init__(self, database_path: str, cipher: SessionCipher) -> None:
        self._database_path = Path(database_path)
        self._cipher = cipher
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._database_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        self._database_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS telegram_accounts (
                    owner_user_id INTEGER PRIMARY KEY,
                    phone TEXT NOT NULL,
                    telegram_user_id INTEGER NOT NULL,
                    username TEXT NOT NULL DEFAULT '',
                    session_encrypted TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS pending_auth (
                    owner_user_id INTEGER PRIMARY KEY,
                    phone TEXT NOT NULL,
                    phone_code_hash TEXT NOT NULL,
                    session_encrypted TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS data_sources (
                    owner_user_id INTEGER PRIMARY KEY,
                    active_source TEXT NOT NULL DEFAULT '',
                    google_sheet_id TEXT NOT NULL DEFAULT '',
                    google_worksheet TEXT NOT NULL DEFAULT 'Sheet1',
                    yandex_csv_url TEXT NOT NULL DEFAULT '',
                    next_index INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS operation_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_user_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    amount INTEGER NOT NULL DEFAULT 0,
                    event_ts INTEGER NOT NULL
                )
                """
            )

    async def get_account(self, owner_user_id: int) -> TelegramAccount | None:
        return await asyncio.to_thread(self._get_account_sync, owner_user_id)

    def _get_account_sync(self, owner_user_id: int) -> TelegramAccount | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM telegram_accounts WHERE owner_user_id = ?",
                (owner_user_id,),
            ).fetchone()

        if row is None:
            return None

        return TelegramAccount(
            owner_user_id=row["owner_user_id"],
            phone=row["phone"],
            telegram_user_id=row["telegram_user_id"],
            username=row["username"],
            session_string=self._cipher.decrypt(row["session_encrypted"]),
        )

    async def save_account(
        self,
        owner_user_id: int,
        phone: str,
        telegram_user_id: int,
        username: str,
        session_string: str,
    ) -> None:
        await asyncio.to_thread(
            self._save_account_sync,
            owner_user_id,
            phone,
            telegram_user_id,
            username,
            session_string,
        )

    def _save_account_sync(
        self,
        owner_user_id: int,
        phone: str,
        telegram_user_id: int,
        username: str,
        session_string: str,
    ) -> None:
        encrypted = self._cipher.encrypt(session_string)
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO telegram_accounts (
                    owner_user_id,
                    phone,
                    telegram_user_id,
                    username,
                    session_encrypted,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(owner_user_id) DO UPDATE SET
                    phone = excluded.phone,
                    telegram_user_id = excluded.telegram_user_id,
                    username = excluded.username,
                    session_encrypted = excluded.session_encrypted,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    owner_user_id,
                    phone,
                    telegram_user_id,
                    username,
                    encrypted,
                ),
            )

    async def get_pending_auth(self, owner_user_id: int) -> PendingAuth | None:
        return await asyncio.to_thread(self._get_pending_auth_sync, owner_user_id)

    def _get_pending_auth_sync(self, owner_user_id: int) -> PendingAuth | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM pending_auth WHERE owner_user_id = ?",
                (owner_user_id,),
            ).fetchone()

        if row is None:
            return None

        return PendingAuth(
            owner_user_id=row["owner_user_id"],
            phone=row["phone"],
            phone_code_hash=row["phone_code_hash"],
            session_string=self._cipher.decrypt(row["session_encrypted"]),
        )

    async def save_pending_auth(
        self,
        owner_user_id: int,
        phone: str,
        phone_code_hash: str,
        session_string: str,
    ) -> None:
        await asyncio.to_thread(
            self._save_pending_auth_sync,
            owner_user_id,
            phone,
            phone_code_hash,
            session_string,
        )

    def _save_pending_auth_sync(
        self,
        owner_user_id: int,
        phone: str,
        phone_code_hash: str,
        session_string: str,
    ) -> None:
        encrypted = self._cipher.encrypt(session_string)
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO pending_auth (
                    owner_user_id,
                    phone,
                    phone_code_hash,
                    session_encrypted,
                    updated_at
                ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(owner_user_id) DO UPDATE SET
                    phone = excluded.phone,
                    phone_code_hash = excluded.phone_code_hash,
                    session_encrypted = excluded.session_encrypted,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (owner_user_id, phone, phone_code_hash, encrypted),
            )

    async def clear_pending_auth(self, owner_user_id: int) -> None:
        await asyncio.to_thread(self._clear_pending_auth_sync, owner_user_id)

    def _clear_pending_auth_sync(self, owner_user_id: int) -> None:
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM pending_auth WHERE owner_user_id = ?",
                (owner_user_id,),
            )

    async def get_source(self, owner_user_id: int) -> SourceConfig:
        return await asyncio.to_thread(self._get_source_sync, owner_user_id)

    def _get_source_sync(self, owner_user_id: int) -> SourceConfig:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM data_sources WHERE owner_user_id = ?",
                (owner_user_id,),
            ).fetchone()

            if row is None:
                connection.execute(
                    "INSERT INTO data_sources (owner_user_id) VALUES (?)",
                    (owner_user_id,),
                )
                row = connection.execute(
                    "SELECT * FROM data_sources WHERE owner_user_id = ?",
                    (owner_user_id,),
                ).fetchone()

        return SourceConfig(
            owner_user_id=row["owner_user_id"],
            active_source=row["active_source"],
            google_sheet_id=row["google_sheet_id"],
            google_worksheet=row["google_worksheet"],
            yandex_csv_url=row["yandex_csv_url"],
            next_index=row["next_index"],
        )

    async def save_google_source(
        self,
        owner_user_id: int,
        sheet_id: str,
        worksheet: str,
    ) -> None:
        await asyncio.to_thread(
            self._save_google_source_sync,
            owner_user_id,
            sheet_id,
            worksheet,
        )

    def _save_google_source_sync(
        self,
        owner_user_id: int,
        sheet_id: str,
        worksheet: str,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO data_sources (
                    owner_user_id,
                    google_sheet_id,
                    google_worksheet,
                    updated_at
                ) VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(owner_user_id) DO UPDATE SET
                    google_sheet_id = excluded.google_sheet_id,
                    google_worksheet = excluded.google_worksheet,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (owner_user_id, sheet_id, worksheet),
            )

    async def save_yandex_source(self, owner_user_id: int, csv_url: str) -> None:
        await asyncio.to_thread(self._save_yandex_source_sync, owner_user_id, csv_url)

    def _save_yandex_source_sync(self, owner_user_id: int, csv_url: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO data_sources (
                    owner_user_id,
                    yandex_csv_url,
                    updated_at
                ) VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(owner_user_id) DO UPDATE SET
                    yandex_csv_url = excluded.yandex_csv_url,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (owner_user_id, csv_url),
            )

    async def set_active_source(self, owner_user_id: int, source_name: str) -> None:
        await asyncio.to_thread(self._set_active_source_sync, owner_user_id, source_name)

    def _set_active_source_sync(self, owner_user_id: int, source_name: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO data_sources (
                    owner_user_id,
                    active_source,
                    updated_at
                ) VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(owner_user_id) DO UPDATE SET
                    active_source = excluded.active_source,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (owner_user_id, source_name),
            )

    async def set_next_index(self, owner_user_id: int, next_index: int) -> None:
        await asyncio.to_thread(self._set_next_index_sync, owner_user_id, next_index)

    def _set_next_index_sync(self, owner_user_id: int, next_index: int) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO data_sources (
                    owner_user_id,
                    next_index,
                    updated_at
                ) VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(owner_user_id) DO UPDATE SET
                    next_index = excluded.next_index,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (owner_user_id, max(0, next_index)),
            )

    async def reset_next_index(self, owner_user_id: int) -> None:
        await self.set_next_index(owner_user_id, 0)

    async def log_action(self, owner_user_id: int, action: str, amount: int = 0) -> None:
        await asyncio.to_thread(self._log_action_sync, owner_user_id, action, amount)

    def _log_action_sync(self, owner_user_id: int, action: str, amount: int) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO operation_events (owner_user_id, action, amount, event_ts)
                VALUES (?, ?, ?, CAST(strftime('%s', 'now') AS INTEGER))
                """,
                (owner_user_id, action, amount),
            )

    async def get_operation_stats(
        self,
        owner_user_id: int,
        action: str,
        within_seconds: int,
    ) -> OperationStats:
        return await asyncio.to_thread(
            self._get_operation_stats_sync,
            owner_user_id,
            action,
            within_seconds,
        )

    def _get_operation_stats_sync(
        self,
        owner_user_id: int,
        action: str,
        within_seconds: int,
    ) -> OperationStats:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    COUNT(*) AS event_count,
                    COALESCE(SUM(amount), 0) AS total_amount,
                    MAX(event_ts) AS last_event_ts
                FROM operation_events
                WHERE owner_user_id = ?
                  AND action = ?
                  AND event_ts >= CAST(strftime('%s', 'now') AS INTEGER) - ?
                """,
                (owner_user_id, action, within_seconds),
            ).fetchone()

        return OperationStats(
            count=int(row["event_count"] or 0),
            amount=int(row["total_amount"] or 0),
            last_event_ts=(
                int(row["last_event_ts"])
                if row["last_event_ts"] is not None
                else None
            ),
        )