# 서빙할 모델 목록과 상태: 기능 폴더의 serving/predictor.py를 불러오고, 1분마다 새 기능 폴더와 바뀐 champion을 반영함
# (이미 불러온 모델의 서빙 코드를 고쳤을 때는 반쯤 바뀐 코드가 섞이지 않도록 서빙을 재시작함)
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from common.config.model_specs import fetch_model_dir, import_model_module, list_model_names, load_model_spec
from common.model_registry.registry import fetch_champion_version
from common.model_registry.tracking import download_run_artifacts
from common.storage.schemas import apply_model_schema
from common.storage.feature_store import connect_feature_store

MODEL_REFRESH_SECONDS = 60
# 모델 이름 -> 그 모델의 서빙 상태 (spec, predictor, store, lock, champion, prediction, refresh_error, signal_error)
serving_models = {}
# 모델 이름 -> 불러오지 못한 이유. 다음 주기에 다시 시도하고 /health에 보여 줌
model_load_errors = {}
# Feast 프로젝트는 플랫폼에 하나라서 모든 모델이 같은 연결을 씀
store = connect_feature_store()


def list_serving_model_names():
    # serving/predictor.py가 있는 모델만 서빙함 (아직 학습만 하는 모델은 빠짐)
    return [name for name in list_model_names() if (fetch_model_dir(name) / "serving" / "predictor.py").exists()]


def load_serving_model(name):
    spec = load_model_spec(name)
    return {
        "spec": spec,
        "predictor": import_model_module(name, "serving.predictor"),
        "store": store,
        "lock": threading.Lock(),
    }


def add_new_models():
    # 서빙 시작 때와 1분마다 불림: 아직 목록에 없는 기능 폴더의 예측 기록 표를 만들고 predictor.py를 불러옴.
    # 한 모델이 실패해도 다른 모델은 그대로 서빙함
    for name in list_serving_model_names():
        if name in serving_models:
            continue
        try:
            apply_model_schema(name)
            serving_models[name] = load_serving_model(name)
            model_load_errors.pop(name, None)
        except Exception as error:
            model_load_errors[name] = repr(error)


def load_champion_model(model, version, run_id):
    model_dir = download_run_artifacts(run_id, Path(tempfile.mkdtemp()))
    return {
        "version": version,
        "run_id": run_id,
        "loaded_at": datetime.now(timezone.utc).isoformat(),
        "model": model["predictor"].load_model(model_dir),
    }


def refresh_champion_models():
    # 새 기능 폴더를 더하고, champion이 바뀌면 1분 안에 새 모델로 바꿈. MLflow가 잠시 안 되면 다음 주기에 다시 시도하고 /health에 오류를 보여 줌
    # 요청을 처리하는 스레드와 목록을 같이 쓰므로 복사본(list)으로 돎
    while True:
        add_new_models()
        for name, model in list(serving_models.items()):
            try:
                champion = fetch_champion_version(name)
                if champion is not None and model.get("champion", {}).get("version") != champion[0]:
                    model["champion"] = load_champion_model(model, *champion)
                model.pop("refresh_error", None)
            except Exception as error:
                model["refresh_error"] = repr(error)
        time.sleep(MODEL_REFRESH_SECONDS)
