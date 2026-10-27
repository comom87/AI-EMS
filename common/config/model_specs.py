# 모델 명세(sources/<기능>/models/model.yaml)를 찾고 검사하고, 모델의 기능 폴더 코드를 불러옴
import importlib
import re

import yaml

from common.config.settings import PROJECT_ROOT
from common.config.source_specs import list_data_source_names

# 기능별 폴더: sources/<기능>/ (모델 설정 models/, 파이프라인 pipelines/, 학습·평가·서빙 코드, 피처 묶음). 기능 하나에 모델 하나
FUNCTIONS_DIR = PROJECT_ROOT / "sources"
# 기능 폴더 안의 모델 명세 위치. 학습 설정은 같은 폴더의 configs/
MODEL_SPEC_FILE = "models/model.yaml"

# 모델 이름(= 기능 폴더 이름)은 MLflow 이름, DB 스키마, Airflow DAG 이름 앞부분으로도 쓰임.
# 공용·기본 스키마와 데이터 소스 이름(데이터 소스도 같은 이름의 스키마를 씀)은 쓸 수 없음
MODEL_NAME_PATTERN = re.compile(r"[a-z][a-z0-9_]*")
RESERVED_MODEL_NAMES = {"common", "public"}

REQUIRED_FIELDS = {
    "owner": [],
    "data_source": [],
    "training": ["image", "config", "data_dir", "cooldown_days"],
    "evaluation": ["metric", "lower_is_better"],
    "monitoring": ["metric_column", "aggregation", "window", "tolerance"],
    "drift": ["window", "psi_threshold", "out_of_range_threshold", "retrain_on"],
}


def find_missing_fields(spec):
    missing = []
    for section, fields in REQUIRED_FIELDS.items():
        if section not in spec:
            missing.append(section)
            continue
        missing.extend(f"{section}.{field}" for field in fields if field not in spec[section])
    return missing


def fetch_model_dirs():
    # 모델 이름 -> 그 모델의 기능 폴더. 기능 하나에 모델 하나라서 기능 폴더 이름이 곧 모델 이름
    return {path.parents[1].name: path.parents[1] for path in sorted(FUNCTIONS_DIR.glob(f"*/{MODEL_SPEC_FILE}"))}


def fetch_model_dir(name):
    model_dirs = fetch_model_dirs()
    if name not in model_dirs:
        raise ValueError(f"Unknown model {name}; add sources/{name}/{MODEL_SPEC_FILE}")
    return model_dirs[name]


def load_model_spec(name):
    # 기능 폴더 이름을 spec["function"]으로도 넣어 둠 (학습·분포 점검 코드를 그 폴더에서 찾음)
    model_dir = fetch_model_dir(name)
    spec_path = f"sources/{model_dir.name}/{MODEL_SPEC_FILE}"
    with open(model_dir / MODEL_SPEC_FILE) as f:
        spec = yaml.safe_load(f)
    missing = find_missing_fields(spec)
    if missing:
        raise ValueError(f"{spec_path} is missing {', '.join(missing)}")
    if not MODEL_NAME_PATTERN.fullmatch(name):
        raise ValueError(f"sources/{name}/ must use only lowercase letters, digits and underscores, starting with a letter")
    if name in RESERVED_MODEL_NAMES or name in list_data_source_names():
        raise ValueError(f"sources/{name}/ is reserved for a database schema; choose another function folder name")
    # 모델 이름은 기능 폴더 이름 (MLflow 실험·등록 모델, DB 스키마, DAG 이름 앞부분)
    spec["name"] = name
    if spec["data_source"] not in list_data_source_names():
        raise ValueError(f"{spec_path} uses unknown data source {spec['data_source']}; see data/")
    spec["function"] = model_dir.name
    # 파이프라인 조립이 데이터 소스와 모델을 구분할 때 씀
    spec["kind"] = "model"
    return spec


def load_function_model_spec(function):
    # 기능 폴더 안의 코드가 자기 모델 명세를 읽을 때 (예: serving/predictor.py)
    return load_model_spec(function)


def list_model_names():
    return sorted(fetch_model_dirs())


def import_model_module(name, module):
    # 모델의 기능 폴더 안 모듈 (예: serving.predictor → sources/<기능>/serving/predictor.py)
    return importlib.import_module(f"sources.{fetch_model_dir(name).name}.{module}")


def import_function_module(function, module):
    # 모델이 수행하는 기능의 코드 sources/<기능>/<module>.py (예: training.model_train, monitoring.input_drift). 파이프라인 YAML의 함수도 여기서 찾음
    return importlib.import_module(f"sources.{function}.{module}")
