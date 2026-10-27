# 모든 모델이 같이 쓰는 서빙 앱. 프로세스 하나가 serving/predictor.py가 있는 모든 기능 폴더(sources/<기능>/)의 모델 champion을 서빙함
# 실행: python3 -m serving.app  (포트는 .env의 AIEMS_SERVING_PORT)
# 호출: POST /models/<모델 이름>/predict, GET /models/<모델 이름>, GET /models, GET /health
# 예측은 외부 시스템이 predict를 부를 때 일어나고, 새 예측을 저장하면 Airflow에 신호를 보내 <모델>_monitoring DAG를 실행함
# 기능 폴더의 serving/predictor.py가 load_model, fetch_latest_scenario, predict_scenario, calculate_metrics, format_predictions, PREDICTION_TABLES를 가져야 함
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import requests
import uvicorn
from fastapi import FastAPI, HTTPException

from common.config.settings import read_setting
from common.storage.schemas import apply_model_schema
from serving.models import list_serving_model_names, load_serving_model, refresh_champion_models, serving_models
from serving.predictions import (
    connect_prediction_db,
    fetch_saved_prediction,
    record_prediction_request,
    record_predictions,
)
from serving.signals import send_predictions_signal

engine = connect_prediction_db()


@asynccontextmanager
async def start_serving(app):
    # 예측을 저장할 표(sources/<기능>/storage/schema.sql)를 먼저 만들고 모델을 불러옴
    for name in list_serving_model_names():
        apply_model_schema(name)
        serving_models[name] = load_serving_model(name)
    threading.Thread(target=refresh_champion_models, daemon=True).start()
    yield


app = FastAPI(title="AI-EMS serving", lifespan=start_serving)


def fetch_serving_model(name):
    if name not in serving_models:
        raise HTTPException(status_code=404, detail=f"Unknown model {name}")
    model = serving_models[name]
    if "champion" not in model:
        raise HTTPException(status_code=503, detail=f"No champion model loaded for {name}")
    return model


def notify_monitoring(name, model):
    # 신호가 실패해도 예측 응답은 그대로 돌려주고, 실패는 /health의 signal_error로 보여줌
    try:
        send_predictions_signal(name)
        model.pop("signal_error", None)
    except requests.RequestException as error:
        model["signal_error"] = repr(error)


def create_prediction(name, model, champion, scenario):
    predictor = model["predictor"]
    saved = fetch_saved_prediction(engine, name, predictor.PREDICTION_TABLES, scenario, champion["version"])
    if saved is None:
        predictions = predictor.predict_scenario(champion["model"], model["store"], scenario)
        metrics = predictor.calculate_metrics(predictions)
        predicted_at = record_predictions(engine, name, predictions, scenario, champion["version"], metrics)
        notify_monitoring(name, model)
    else:
        predictions, predicted_at = saved
    return {
        "scenario": scenario,
        "model_version": champion["version"],
        "predicted_at": predicted_at.isoformat(),
        **predictor.format_predictions(predictions),
    }


@app.post("/models/{name}/predict")
def predict_latest_scenario(name: str):
    # 가장 최근에 들어온 입력으로 예측. 같은 입력·모델 버전이면 저장된 예측을 돌려줌
    model = fetch_serving_model(name)
    requested_at = datetime.now(timezone.utc)
    started = time.perf_counter()
    champion = model["champion"]
    scenario = model["predictor"].fetch_latest_scenario(model["store"])
    with model["lock"]:
        last = model.get("prediction")
        cached = last is not None and last["scenario"] == scenario and last["model_version"] == champion["version"]
        if not cached:
            model["prediction"] = create_prediction(name, model, champion, scenario)
        prediction = model["prediction"]
    record_prediction_request(
        engine, name, requested_at, (time.perf_counter() - started) * 1000, scenario, champion["version"], cached,
    )
    return prediction


@app.get("/models/{name}")
def describe_model(name: str):
    champion = fetch_serving_model(name)["champion"]
    return {"name": name, **{key: champion[key] for key in ["version", "run_id", "loaded_at"]}}


@app.get("/models")
def list_models():
    return sorted(serving_models)


@app.get("/health")
def check_health():
    return {
        "status": "ok",
        "models": {
            name: {
                "model_loaded": "champion" in model,
                "refresh_error": model.get("refresh_error"),
                "signal_error": model.get("signal_error"),
            }
            for name, model in serving_models.items()
        },
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(read_setting("AIEMS_SERVING_PORT")))
