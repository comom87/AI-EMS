import os
from pathlib import Path

# common/config/settings.py에서 두 단계 위가 저장소 루트
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def read_env_file(path):
    if not path.exists():
        return {}
    values = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    return values


def read_setting(name):
    if name in os.environ:
        return os.environ[name]
    values = read_env_file(PROJECT_ROOT / ".env")
    if name not in values:
        raise KeyError(f"{name} is not set; add it to .env in the project root")
    return values[name]


DATABASE_URL = read_setting("AIEMS_DATABASE_URL")
MLFLOW_TRACKING_URI = read_setting("AIEMS_MLFLOW_TRACKING_URI")
