# 서빙할 모델 목록과 상태: 기능 폴더의 serving/predictor.py를 불러오고, champion이 바뀌면 새 모델로 바꿈
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from common.config.model_specs import fetch_model_dir, import_model_module, list_model_names, load_model_spec
from common.model_registry.registry import fetch_champion_version
from common.model_registry.tracking import download_run_artifacts
from common.storage.feature_store import connect_feature_store

MODEL_REFRESH_SECONDS = 60
# 모델 이름 -> 그 모델의 서빙 상태 (spec, predictor, store, lock, champion, prediction, refresh_error)
serving_models = {}
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


def load_champion_model(model, version, run_id):
    model_dir = download_run_artifacts(run_id, Path(tempfile.mkdtemp()))
    return {
        "version": version,
        "run_id": run_id,
        "loaded_at": datetime.now(timezone.utc).isoformat(),
        "model": model["predictor"].load_model(model_dir),
    }


def refresh_champion_models():
    # champion이 바뀌면 1분 안에 새 모델로 바꿈. MLflow가 잠시 안 되면 다음 주기에 다시 시도하고 /health에 오류를 보여 줌
    while True:
        for name, model in serving_models.items():
            try:
                champion = fetch_champion_version(name)
                if champion is not None and model.get("champion", {}).get("version") != champion[0]:
                    model["champion"] = load_champion_model(model, *champion)
                model.pop("refresh_error", None)
            except Exception as error:
                model["refresh_error"] = repr(error)
        time.sleep(MODEL_REFRESH_SECONDS)
