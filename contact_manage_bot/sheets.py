import csv
import io
import re
import urllib.parse
from dataclasses import dataclass

import aiohttp

from .storage import SourceConfig


GOOGLE_SHEET_ID_RE = re.compile(r"^[a-zA-Z0-9-_]{20,}$")
GOOGLE_SHEET_URL_RE = re.compile(
    r"^https://docs\.google\.com/spreadsheets/d/([a-zA-Z0-9-_]+)"
)


@dataclass
class ContactRow:
    first_name: str
    username: str
    phone: str


def _normalize_username(value: str) -> str:
    value = (value or "").strip()
    if value.startswith("@"):
        value = value[1:]
    return value


def _normalize_phone(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    cleaned = re.sub(r"[^0-9+]", "", value)
    if cleaned.startswith("00"):
        cleaned = "+" + cleaned[2:]
    return cleaned


def parse_google_sheet_input(raw_value: str) -> str:
    value = (raw_value or "").strip()
    if not value:
        raise ValueError("Google Sheet ID или ссылка не указаны")

    match = GOOGLE_SHEET_URL_RE.search(value)
    if match:
        return match.group(1)

    if GOOGLE_SHEET_ID_RE.fullmatch(value):
        return value

    raise ValueError(
        "Некорректный Google Sheet ID или ссылка. Используйте ID таблицы или ссылку вида https://docs.google.com/spreadsheets/d/..."
    )


def _validate_contact_header(rows: list[list[str]]) -> None:
    if not rows:
        return

    header = [cell.strip().lower() for cell in rows[0][:3]]
    if header != ["name", "nickname", "phone"]:
        raise ValueError(
            "Неверный заголовок таблицы. Первая строка должна быть: name, nickname, phone"
        )


def _rows_to_contacts(rows: list[list[str]]) -> list[ContactRow]:
    contacts: list[ContactRow] = []
    for raw in rows:
        name = raw[0].strip() if len(raw) > 0 and raw[0] else ""
        username = _normalize_username(raw[1] if len(raw) > 1 else "")
        phone = _normalize_phone(raw[2] if len(raw) > 2 else "")

        if not username and not phone:
            continue

        contacts.append(ContactRow(first_name=name, username=username, phone=phone))
    return contacts


async def _read_google_rows(source: SourceConfig) -> list[list[str]]:
    """Read a public Google Sheet via CSV export (no service account needed).

    The sheet must be shared as 'Anyone with the link can view'.
    """
    sheet_name = urllib.parse.quote(source.google_worksheet)
    url = (
        f"https://docs.google.com/spreadsheets/d/{source.google_sheet_id}"
        f"/gviz/tq?tqx=out:csv&sheet={sheet_name}"
    )

    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status == 404:
                raise ValueError(
                    "Таблица не найдена. Проверьте ссылку/ID таблицы."
                )
            if resp.status in (401, 403):
                raise ValueError(
                    "Нет доступа к таблице. Убедитесь, что включен доступ "
                    "«Все, у кого есть ссылка» → «Читатель»."
                )
            if resp.status != 200:
                raise ValueError(
                    f"Google вернул ошибку (HTTP {resp.status}). "
                    "Проверьте ссылку и настройки доступа."
                )
            body = await resp.text(encoding="utf-8")

    reader = csv.reader(io.StringIO(body))
    values = list(reader)
    _validate_contact_header(values)
    if not values:
        return []
    return values[1:]


async def _read_yandex_csv_rows(source: SourceConfig) -> list[list[str]]:
    if not source.yandex_csv_url:
        raise ValueError("Yandex CSV URL is not configured")

    async with aiohttp.ClientSession() as session:
        async with session.get(source.yandex_csv_url, timeout=aiohttp.ClientTimeout(total=30)) as response:
            response.raise_for_status()
            body = await response.text(encoding="utf-8")

    reader = csv.reader(io.StringIO(body))
    values = list(reader)
    _validate_contact_header(values)
    if not values:
        return []

    return values[1:]


async def validate_google_source(
    google_sheet_id: str,
    worksheet: str,
) -> int:
    source = SourceConfig(
        owner_user_id=0,
        active_source="google",
        google_sheet_id=google_sheet_id,
        google_worksheet=worksheet,
        yandex_csv_url="",
        next_index=0,
    )
    rows = await _read_google_rows(source)
    return len(rows)


async def validate_yandex_source(yandex_csv_url: str) -> int:
    source = SourceConfig(
        owner_user_id=0,
        active_source="yandex_csv",
        google_sheet_id="",
        google_worksheet="Sheet1",
        yandex_csv_url=yandex_csv_url,
        next_index=0,
    )
    rows = await _read_yandex_csv_rows(source)
    return len(rows)


async def load_contacts(source: SourceConfig) -> list[ContactRow]:
    source_name = source.active_source.lower().strip()
    if source_name == "google":
        rows = await _read_google_rows(source)
    elif source_name == "yandex_csv":
        rows = await _read_yandex_csv_rows(source)
    else:
        raise ValueError("Active source is not configured. Use google or yandex_csv.")

    return _rows_to_contacts(rows)
