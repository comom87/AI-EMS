# 서빙 앱이 모델 스키마(<모델 이름>.*)에 예측과 요청을 저장하고 다시 읽는 함수들
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text

from common.config.settings import DATABASE_URL


def connect_prediction_db():
    return create_engine(DATABASE_URL)


def fetch_saved_prediction(engine, model_name, tables, scenario, model_version):
    # 같은 시나리오·모델 버전의 첫 예측. 서빙을 다시 띄워도 같은 예측을 돌려주려고 씀
    params = {"scenario": scenario, "version": int(model_version)}
    predicted_at = pd.read_sql(
        text(
            f"SELECT MIN(predicted_at) FROM {model_name}.prediction_metrics "
            "WHERE scenario = :scenario AND model_version = :version"
        ),
        engine,
        params=params,
    ).iloc[0, 0]
    if predicted_at is None or pd.isna(predicted_at):
        return None
    predictions = {
        table: pd.read_sql(
            text(
                f"SELECT * FROM {model_name}.{table} "
                f"WHERE scenario = :scenario AND model_version = :version AND predicted_at = :predicted_at ORDER BY {order}"
            ),
            engine,
            params={**params, "predicted_at": predicted_at},
        )
        for table, order in tables.items()
    }
    return predictions, predicted_at


def record_predictions(engine, model_name, predictions, scenario, model_version, metrics):
    predicted_at = datetime.now(timezone.utc)
    for table, frame in predictions.items():
        frame.assign(model_version=int(model_version), predicted_at=predicted_at).to_sql(
            table, engine, schema=model_name, if_exists="append", index=False
        )
    pd.DataFrame(
        [{"scenario": scenario, "model_version": int(model_version), "predicted_at": predicted_at, **metrics}]
    ).to_sql("prediction_metrics", engine, schema=model_name, if_exists="append", index=False)
    return predicted_at


def record_prediction_request(engine, model_name, requested_at, latency_ms, scenario, model_version, cached):
    pd.DataFrame(
        [{
            "requested_at": requested_at,
            "latency_ms": latency_ms,
            "scenario": scenario,
            "model_version": int(model_version),
            "cached": cached,
        }]
    ).to_sql("prediction_requests", engine, schema=model_name, if_exists="append", index=False)
