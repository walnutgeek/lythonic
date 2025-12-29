"""
File system utilities.

Helper functions for working with directories and paths.

```python
from lythonic.misc import ensure_dir, tabula_rasa_path
from pathlib import Path

# Create directory if it doesn't exist
data_dir = ensure_dir("./data/cache")

# Clear a path (file or directory) and ensure parent exists
output = tabula_rasa_path("./output/results.json")
```
"""

import shutil
from pathlib import Path


def ensure_dir(dir: Path | str) -> Path:
    """Create directory and parents if they don't exist. Returns the Path."""
    dir = Path(dir)
    dir.mkdir(parents=True, exist_ok=True)
    return dir


def tabula_rasa_path(p: Path | str) -> Path:
    """
    Ensure path doesn't exist but parent directory does.

    Removes the file or directory at the path if it exists, then ensures
    the parent directory exists. Returns the Path ready for writing.
    """
    p = Path(p)
    if p.exists():
        if p.is_dir():
            shutil.rmtree(p)
        else:
            p.unlink()
    if not p.parent.is_dir():
        ensure_dir(p.parent)
    return p
