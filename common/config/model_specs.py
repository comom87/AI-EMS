# 모델 명세(sources/<기능>/models/model.yaml)를 찾고 검사하고, 모델의 기능 폴더 코드를 불러옴
import importlib
import re

import yaml

from common.config.settings import PROJECT_ROOT
from common.config.source_specs import list_data_source_names

# 기능별 폴더: sources/<기능>/ (학습·평가·서빙 코드, 피처 묶음, 모델 설정 models/). 기능 하나에 모델 하나
FUNCTIONS_DIR = PROJECT_ROOT / "sources"
# 기능 폴더 안의 모델 명세 위치. 학습 설정은 같은 폴더의 configs/
MODEL_SPEC_FILE = "models/model.yaml"
# 학습 DAG가 부르는 기능 폴더의 코드. 없으면 명세 검사에서 막음
TRAINING_MODULES = ["training/data.py", "training/jobs.py"]

# 모델 이름은 MLflow 이름, DB 스키마, Airflow DAG 이름 앞부분으로도 쓰임.
# 공용·기본 스키마와 데이터 소스 이름(데이터 소스도 같은 이름의 스키마를 씀)은 쓸 수 없음
MODEL_NAME_PATTERN = re.compile(r"[a-z][a-z0-9_]*")
RESERVED_MODEL_NAMES = {"common", "public"}

REQUIRED_FIELDS = {
    "name": [],
    "owner": [],
    "data_source": [],
    "training": ["image", "config", "data_dir", "schedule", "retries", "cooldown_days"],
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
    # 모델 이름 -> 그 모델의 기능 폴더 (sources/<기능>/models/model.yaml의 name). 모델 이름은 폴더 이름과 다를 수 있음
    model_dirs = {}
    for path in sorted(FUNCTIONS_DIR.glob(f"*/{MODEL_SPEC_FILE}")):
        with open(path) as f:
            name = yaml.safe_load(f)["name"]
        function_dir = path.parents[1]
        if name in model_dirs:
            raise ValueError(f"Model name {name} is used by both {model_dirs[name].name} and {function_dir.name}")
        model_dirs[name] = function_dir
    return model_dirs


def fetch_model_dir(name):
    model_dirs = fetch_model_dirs()
    if name not in model_dirs:
        raise ValueError(f"Unknown model {name}; add sources/<function>/{MODEL_SPEC_FILE} with name: {name}")
    return model_dirs[name]


def load_model_spec(name):
    # 기능 폴더 이름을 spec["function"]으로 넣어 둠 (학습·분포 점검 코드를 그 폴더에서 찾음)
    model_dir = fetch_model_dir(name)
    spec_path = f"sources/{model_dir.name}/{MODEL_SPEC_FILE}"
    with open(model_dir / MODEL_SPEC_FILE) as f:
        spec = yaml.safe_load(f)
    missing = find_missing_fields(spec)
    if missing:
        raise ValueError(f"{spec_path} is missing {', '.join(missing)}")
    if not MODEL_NAME_PATTERN.fullmatch(name):
        raise ValueError(f"{name} must use only lowercase letters, digits and underscores, starting with a letter")
    if name in RESERVED_MODEL_NAMES or name in list_data_source_names():
        raise ValueError(f"{name} is reserved for a database schema; choose another model name")
    if spec["data_source"] not in list_data_source_names():
        raise ValueError(f"{spec_path} uses unknown data source {spec['data_source']}; see data/")
    for module in TRAINING_MODULES:
        if not (model_dir / module).exists():
            raise ValueError(f"sources/{model_dir.name}/ has no {module}")
    spec["function"] = model_dir.name
    return spec


def load_function_model_spec(function):
    # 기능 폴더 안의 코드가 자기 모델 명세를 읽을 때 (예: serving/predictor.py)
    with open(FUNCTIONS_DIR / function / MODEL_SPEC_FILE) as f:
        return load_model_spec(yaml.safe_load(f)["name"])


def list_model_names():
    return sorted(fetch_model_dirs())


def import_model_module(name, module):
    # 모델의 기능 폴더 안 모듈 (예: serving.predictor → sources/<기능>/serving/predictor.py)
    return importlib.import_module(f"sources.{fetch_model_dir(name).name}.{module}")


def import_function_module(function, module):
    # 모델이 수행하는 기능의 코드 sources/<기능>/<module>.py (예: training.jobs, monitoring.input_drift)
    return importlib.import_module(f"sources.{function}.{module}")
