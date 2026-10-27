# 데이터 소스를 찾고, 소스의 처리 코드를 불러옴. data/ 아래 폴더 하나가 데이터 소스 하나이고 폴더 이름이 소스 이름
import importlib
import re

from common.config.settings import PROJECT_ROOT

# 데이터: data/<소스>/ (데이터 하나에 관한 것 전부. 처리 코드는 data/<소스>/handler/)
DATA_DIR = PROJECT_ROOT / "data"
# 소스 이름은 DB 스키마·Feast 이름 앞부분·DAG 이름으로도 쓰임. 이 규칙에 맞지 않는 폴더(__pycache__ 등)는 데이터 소스가 아님
SOURCE_NAME_PATTERN = re.compile(r"[a-z][a-z0-9_]*")


def list_data_source_names():
    return sorted(path.name for path in DATA_DIR.iterdir() if path.is_dir() and SOURCE_NAME_PATTERN.fullmatch(path.name))


def fetch_data_source_dir(name):
    # data/<소스>/: 이 데이터에 관한 것 전부 (pipelines/, storage/(schema.sql, feature_store.py), configs/, handler/, files/)
    if name not in list_data_source_names():
        raise ValueError(f"Unknown data source {name}; add a data/{name}/ folder")
    return DATA_DIR / name


def load_source_spec(name):
    # 파이프라인 함수에 spec으로 넘어감. kind: 파이프라인 조립이 데이터 소스와 모델을 구분할 때 씀 (파이프라인 폴더, 함수를 찾을 곳)
    fetch_data_source_dir(name)
    return {"name": name, "kind": "source"}


def import_data_source_module(name, module):
    # 소스 이름(예: kpg193)으로 그 소스의 처리 코드 data/<소스>/handler/<module>.py를 불러옴
    fetch_data_source_dir(name)
    return importlib.import_module(f"data.{name}.handler.{module}")
