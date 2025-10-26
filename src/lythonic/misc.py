from pathlib import Path


def ensure_dir(dir: Path) -> Path:
    dir.mkdir(parents=True, exist_ok=True)
    return dir


def ensure_file_deleted_but_parent_exists(test_db_path: Path | str) -> Path:
    """make sure that db does not exist, but parent dir does"""
    test_db_path = Path(test_db_path)
    if test_db_path.exists():
        test_db_path.unlink()
    if not test_db_path.parent.is_dir():
        ensure_dir(test_db_path.parent)
    return test_db_path
