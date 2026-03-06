import json
from pathlib import Path


class StateStore:
    def __init__(self, state_file: str) -> None:
        self._path = Path(state_file)

    def _load(self) -> dict:
        if not self._path.exists():
            return {"next_index": 0}
        return json.loads(self._path.read_text(encoding="utf-8"))

    def _save(self, payload: dict) -> None:
        self._path.write_text(
            json.dumps(payload, ensure_ascii=True, indent=2),
            encoding="utf-8",
        )

    async def get_next_index(self) -> int:
        payload = self._load()
        return int(payload.get("next_index", 0))

    async def set_next_index(self, value: int) -> None:
        payload = self._load()
        payload["next_index"] = max(0, int(value))
        self._save(payload)

    async def reset_next_index(self) -> None:
        self._save({"next_index": 0})
