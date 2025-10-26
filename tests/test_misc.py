from pathlib import Path

from lythonic.misc import tabula_rasa_path


def test_tabula_rasa_path():
    build_dir = Path("build")
    dir = tabula_rasa_path(build_dir / "tabula/rasa")
    assert not dir.is_dir()
    file1 = tabula_rasa_path(dir / "test.txt")
    assert dir.is_dir()
    file1.write_text("test")
    assert file1.is_file()
    file2 = tabula_rasa_path(dir / "file_used_to_be")
    file2.write_text("test")
    assert file2.is_file()
    tabula_rasa_path(file1)
    assert not file1.is_file()
    tabula_rasa_path(file2)
    assert not file2.exists()
    tabula_rasa_path(dir)
    assert not dir.is_dir()
