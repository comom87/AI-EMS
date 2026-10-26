# 데이터 소스 명세(data/<소스>/source.yaml)를 찾고 검사하고, 소스의 처리 코드를 불러옴
import importlib

import yaml

from common.config.settings import PROJECT_ROOT

# 데이터: data/<소스>/ (데이터 하나에 관한 것 전부. 처리 코드는 data/<소스>/handler/)
DATA_DIR = PROJECT_ROOT / "data"
SOURCE_SPEC_FILE = "source.yaml"
SOURCE_REQUIRED_FIELDS = ["name", "owner", "description", "schedule", "retries"]


def list_data_source_names():
    # data/<소스>/source.yaml. 소스 이름은 DB 스키마·Feast 이름 앞부분·DAG 이름으로도 쓰임
    return sorted(path.parent.name for path in DATA_DIR.glob(f"*/{SOURCE_SPEC_FILE}"))


def fetch_data_source_dir(name):
    # data/<소스>/: 이 데이터에 관한 것 전부 (source.yaml, schema.sql, configs/, feature_store.py, handler/)
    path = DATA_DIR / name
    if not (path / SOURCE_SPEC_FILE).exists():
        raise ValueError(f"Unknown data source {name}; add data/{name}/{SOURCE_SPEC_FILE}")
    return path


def load_source_spec(name):
    with open(fetch_data_source_dir(name) / SOURCE_SPEC_FILE) as f:
        spec = yaml.safe_load(f)
    missing = [field for field in SOURCE_REQUIRED_FIELDS if field not in spec]
    if missing:
        raise ValueError(f"data/{name}/{SOURCE_SPEC_FILE} is missing {', '.join(missing)}")
    if spec["name"] != name:
        raise ValueError(f"data/{name}/{SOURCE_SPEC_FILE} has name {spec['name']}; it must match the folder name")
    return spec


def import_data_source_module(name, module):
    # 소스 이름(예: kpg193)으로 그 소스의 처리 코드 data/<소스>/handler/<module>.py를 불러옴
    fetch_data_source_dir(name)
    return importlib.import_module(f"data.{name}.handler.{module}")
