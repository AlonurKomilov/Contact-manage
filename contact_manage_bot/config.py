import os
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass
class Settings:
    bot_token: str
    bot_owner_id: int
    tg_api_id: int
    tg_api_hash: str
    batch_size: int
    max_batch_size: int
    sleep_between_requests_sec: float
    request_jitter_sec: float
    import_cooldown_sec: int
    delete_cooldown_sec: int
    login_code_cooldown_sec: int
    database_path: str
    session_secret: str


def _as_int(name: str, default: int | None = None) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        if default is None:
            raise ValueError(f"Environment variable {name} is required")
        return default
    return int(raw)


def _as_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def load_settings() -> Settings:
    load_dotenv()

    return Settings(
        bot_token=os.getenv("BOT_TOKEN", ""),
        bot_owner_id=_as_int("BOT_OWNER_ID", 0),
        tg_api_id=_as_int("TG_API_ID"),
        tg_api_hash=os.getenv("TG_API_HASH", ""),
        batch_size=_as_int("BATCH_SIZE", 200),
        max_batch_size=_as_int("MAX_BATCH_SIZE", 200),
        sleep_between_requests_sec=_as_float("SLEEP_BETWEEN_REQUESTS_SEC", 1.5),
        request_jitter_sec=_as_float("REQUEST_JITTER_SEC", 0.75),
        import_cooldown_sec=_as_int("IMPORT_COOLDOWN_SEC", 1800),
        delete_cooldown_sec=_as_int("DELETE_COOLDOWN_SEC", 21600),
        login_code_cooldown_sec=_as_int("LOGIN_CODE_COOLDOWN_SEC", 60),
        database_path=os.getenv("DATABASE_PATH", "app.db"),
        session_secret=os.getenv("SESSION_SECRET", ""),
    )
