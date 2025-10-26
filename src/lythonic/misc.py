import shutil
from pathlib import Path


def ensure_dir(dir: Path | str) -> Path:
    dir = Path(dir)
    dir.mkdir(parents=True, exist_ok=True)
    return dir


def tabula_rasa_path(p: Path | str) -> Path:
    """Make sure that path does not exists, but parent dir does"""
    p = Path(p)
    if p.exists():
        if p.is_dir():
            shutil.rmtree(p)
        else:
            p.unlink()
    if not p.parent.is_dir():
        ensure_dir(p.parent)
    return p
