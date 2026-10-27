# 서빙 성능 점검: champion의 최근 예측 지표를 오프라인 평가값과 비교함 (<모델>_monitoring DAG가 부름). 테이블 정의는 db/model_monitoring.sql
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from common.model_registry.registry import fetch_champion_version
from common.config.settings import DATABASE_URL
from common.model_registry.tracking import connect_tracking_server


def aggregate_metric(values, aggregation):
    # pooled_rmse: 예측마다의 RMSE를 제곱 평균으로 합침 (전체 오차의 RMSE와 같음), mean: 단순 평균
    if aggregation == "pooled_rmse":
        return float(np.sqrt(np.mean(np.square(values))))
    if aggregation == "mean":
        return float(np.mean(values))
    raise ValueError(f"Unknown monitoring aggregation {aggregation}; use pooled_rmse or mean")


def fetch_offline_baseline(model_name, version):
    # champion을 승격할 때 쓴 평가 run의 지표 (validate_candidate_model이 남긴 태그로 찾음)
    client = connect_tracking_server()
    tags = client.get_model_version(model_name, version).tags
    return client.get_run(tags["validation.eval_run_id"]).data.metrics[tags["validation.metric"]]


def record_performance_check(model_name, model_version, metric_name, recent_value, offline_baseline, degraded):
    pd.DataFrame(
        [{
            "model_name": model_name,
            "checked_at": datetime.now(timezone.utc),
            "model_version": int(model_version),
            "metric_name": metric_name,
            "recent_value": recent_value,
            "offline_baseline": offline_baseline,
            "degraded": degraded,
        }]
    ).to_sql("model_performance_checks", create_engine(DATABASE_URL), schema="common", if_exists="append", index=False)


def check_model_performance(spec):
    # champion의 최근 window건 예측 지표가 오프라인 평가값보다 tolerance 넘게 나쁘면 True. 기록이 모자라면 False
    model_name = spec["name"]
    monitoring = spec["monitoring"]
    champion = fetch_champion_version(model_name)
    if champion is None:
        return False
    version, _ = champion
    values = pd.read_sql(
        text(
            f"SELECT {monitoring['metric_column']} FROM {model_name}.prediction_metrics "
            "WHERE model_version = :version ORDER BY predicted_at DESC LIMIT :window"
        ),
        create_engine(DATABASE_URL),
        params={"version": int(version), "window": monitoring["window"]},
    ).iloc[:, 0]
    if len(values) < monitoring["window"]:
        return False

    recent_value = aggregate_metric(values, monitoring["aggregation"])
    offline_baseline = fetch_offline_baseline(model_name, version)
    if spec["evaluation"]["lower_is_better"]:
        degraded = recent_value > offline_baseline * (1 + monitoring["tolerance"])
    else:
        degraded = recent_value < offline_baseline * (1 - monitoring["tolerance"])
    record_performance_check(model_name, version, monitoring["metric_column"], recent_value, offline_baseline, degraded)
    return degraded

