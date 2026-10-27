# 모델(= 기능 폴더 sources/<기능>/)을 찾고, 모델의 기능 폴더 코드를 불러옴
# 모델마다 설정 파일은 따로 없음: 단계·주기·기준값은 pipelines/*.yaml에,
# 모델 전체가 같이 쓰는 값(데이터 소스, 학습 이미지, 학습 데이터 폴더)은 기능 폴더 코드의 상수(training/dataset_preparation.py 등)에 둠
import importlib
import re

from common.config.settings import PROJECT_ROOT
from common.config.source_specs import list_data_source_names

# 기능별 폴더: sources/<기능>/ (학습 설정 models/configs/, 파이프라인 pipelines/, 학습·평가·서빙 코드, 피처 묶음). 기능 하나에 모델 하나
FUNCTIONS_DIR = PROJECT_ROOT / "sources"

# 모델 이름(= 기능 폴더 이름)은 MLflow 이름, DB 스키마, Airflow DAG 이름 앞부분으로도 쓰임.
# 이 규칙에 맞는 sources/ 아래 폴더가 모두 모델임. 공용·기본 스키마와 데이터 소스 이름(데이터 소스도 같은 이름의 스키마를 씀)은 쓸 수 없음
MODEL_NAME_PATTERN = re.compile(r"[a-z][a-z0-9_]*")
RESERVED_MODEL_NAMES = {"common", "public"}


def fetch_model_dirs():
    # 모델 이름 -> 그 모델의 기능 폴더. 기능 하나에 모델 하나라서 기능 폴더 이름이 곧 모델 이름
    return {
        path.name: path
        for path in sorted(FUNCTIONS_DIR.iterdir())
        if path.is_dir() and MODEL_NAME_PATTERN.fullmatch(path.name)
    }


def fetch_model_dir(name):
    model_dirs = fetch_model_dirs()
    if name not in model_dirs:
        raise ValueError(f"Unknown model {name}; add sources/{name}/")
    return model_dirs[name]


def load_model_spec(name):
    # 파이프라인 조립, operator, 기능 폴더 함수가 첫 인자로 받는 spec
    # function: 학습·분포 점검 코드를 찾을 기능 폴더, kind: 파이프라인 조립이 데이터 소스·플랫폼과 모델을 구분할 때 씀
    model_dir = fetch_model_dir(name)
    if name in RESERVED_MODEL_NAMES or name in list_data_source_names():
        raise ValueError(f"sources/{name}/ is reserved for a database schema; choose another function folder name")
    return {"name": name, "function": model_dir.name, "kind": "model"}


def list_model_names():
    return sorted(fetch_model_dirs())


def import_model_module(name, module):
    # 모델의 기능 폴더 안 모듈 (예: serving.predictor → sources/<기능>/serving/predictor.py)
    return importlib.import_module(f"sources.{fetch_model_dir(name).name}.{module}")


def import_function_module(function, module):
    # 모델이 수행하는 기능의 코드 sources/<기능>/<module>.py (예: training.model_train, monitoring.input_drift). 파이프라인 YAML의 함수도 여기서 찾음
    return importlib.import_module(f"sources.{function}.{module}")
