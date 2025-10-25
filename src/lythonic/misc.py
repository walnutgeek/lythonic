import json
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any, NamedTuple


class EnsureJson(NamedTuple):
    name: str
    source_dir: str
    target_dir: str
    backward_fn: Callable[[Any], Any] | None

    def forward(self) -> "EnsureJson":
        """copy file forward if it does not exist"""
        target = Path(self.target_dir) / self.name
        source = Path(self.source_dir) / self.name
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            print(f"Copy forward: {source=} {target=}")
            shutil.copy(source, target)
        return self

    def backward(self) -> "EnsureJson":
        """copy file backward if target content is different then source.
        `backward_fn()` is necessary sanitize json content to ensure no sensitive
        info will be checked."""
        target = Path(self.target_dir) / self.name
        source = Path(self.source_dir) / self.name
        if target.exists() and source.exists():
            source_json, target_json = (json.loads(f.read_text()) for f in (source, target))
            if self.backward_fn is not None:
                target_json = self.backward_fn(target_json)
            if json.dumps(source_json) != json.dumps(target_json):
                print(f"Write backward: {source=} {target=}")
                source.write_text(json.dumps(target_json, indent=4))
        return self


def ensure_dir(dir: Path) -> Path:
    dir.mkdir(parents=True, exist_ok=True)
    return dir


def ensure_file_deleted_but_parent_exists(test_db_path: Path | str) -> Path:
    """make sure that db does not exist, but parent dir does"""
    test_db_path = Path(test_db_path)
    if test_db_path.exists():
        test_db_path.unlink()
    if not test_db_path.parent.is_dir():
        test_db_path.parent.mkdir(parents=True, exist_ok=True)
    return test_db_path
