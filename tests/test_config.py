import os
from pathlib import Path
import subprocess
import sys


def test_configuration_import_is_independent_of_working_directory():
    project_root = Path(__file__).resolve().parents[1]
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(project_root)
    result = subprocess.run(
        [sys.executable, "-c", "import config.config as c; print(c.CONFIG_PATH.name)"],
        cwd=project_root.parent,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "config.ini"
