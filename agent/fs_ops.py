from pathlib import Path
from typing import List


class FileSandbox:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    def _safe_path(self, path: str) -> Path:
        candidate = (self.root / path).resolve()
        try:
            candidate.relative_to(self.root)
        except Exception:
            raise ValueError("Path outside sandbox")
        return candidate

    def read_text(self, path: str) -> str:
        p = self._safe_path(path)
        return p.read_text(encoding="utf-8")

    def write_text(self, path: str, content: str) -> None:
        p = self._safe_path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")

    def mkdir(self, path: str) -> None:
        p = self._safe_path(path)
        p.mkdir(parents=True, exist_ok=True)

    def rm(self, path: str) -> None:
        p = self._safe_path(path)
        if p.is_dir():
            for child in p.rglob("*"):
                if child.is_file():
                    child.unlink()
            for child in sorted(p.rglob("*"), reverse=True):
                if child.is_dir():
                    child.rmdir()
            p.rmdir()
        elif p.exists():
            p.unlink()

    def ls(self, path: str = ".") -> List[str]:
        p = self._safe_path(path)
        if not p.exists():
            return []
        return [str(x.relative_to(self.root)) for x in p.iterdir()]
