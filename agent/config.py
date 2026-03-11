import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

CONFIG_FILE = Path("config.json")


@dataclass
class DBConfig:
    user_id: str = ""
    host: str = ""
    port: str = "5432"
    base: str = ""

    def is_complete(self) -> bool:
        return bool(self.user_id and self.host and self.port and self.base)


class ConfigStore:
    def __init__(self, path: Optional[Path] = None) -> None:
        self.path = path or CONFIG_FILE

    def load(self) -> DBConfig:
        if not self.path.exists():
            return DBConfig()
        data = json.loads(self.path.read_text(encoding="utf-8"))
        return DBConfig(**data)

    def save(self, cfg: DBConfig) -> None:
        self.path.write_text(json.dumps(asdict(cfg), ensure_ascii=True, indent=2), encoding="utf-8")
