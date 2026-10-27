# 입력 분포 점검: 최근 입력을 champion의 학습 데이터와 비교함.
# 무엇을 비교할지는 모델이 수행하는 기능의 코드(sources/<기능>/monitoring/input_drift.py)가 정하고, 여기서는 판정과 기록만 함
import json
from datetime import datetime, timezone

import numpy as np
from sqlalchemy import create_engine, text

from common.model_registry.registry import fetch_champion_version
from common.config.settings import DATABASE_URL
from common.config.model_specs import import_function_module
from common.model_registry.tracking import connect_tracking_server, fetch_run_id

PSI_BINS = 10
PSI_EPSILON = 1e-4


def compute_psi(reference, current):
    # PSI(Population Stability Index): 학습 데이터 분위수로 나눈 구간에서 두 분포의 비율 차이
    edges = np.unique(np.quantile(reference, np.linspace(0, 1, PSI_BINS + 1)[1:-1]))
    bins = len(edges) + 1
    reference_ratio = np.bincount(np.searchsorted(edges, reference, side="right"), minlength=bins) / len(reference)
    current_ratio = np.bincount(np.searchsorted(edges, current, side="right"), minlength=bins) / len(current)
    reference_ratio = np.clip(reference_ratio, PSI_EPSILON, None)
    current_ratio = np.clip(current_ratio, PSI_EPSILON, None)
    return float(np.sum((current_ratio - reference_ratio) * np.log(current_ratio / reference_ratio)))


def classify_data_drift(measures, drift_spec):
    # out_of_range: 학습 범위 밖 값이 많음, shifted: 범위 안이지만 분포가 바뀜, stable: 변화 없음
    if measures["out_of_range_ratio"] > drift_spec["out_of_range_threshold"]:
        return "out_of_range"
    if max(value for name, value in measures.items() if name.endswith("_psi")) >= drift_spec["psi_threshold"]:
        return "shifted"
    return "stable"


def record_drift_check(model_name, model_version, first_scenario, last_scenario, status, measures):
    with create_engine(DATABASE_URL).begin() as connection:
        connection.execute(
            text(
                "INSERT INTO common.model_drift_checks "
                "(model_name, checked_at, model_version, first_scenario, last_scenario, status, measures) "
                "VALUES (:model_name, :checked_at, :model_version, :first, :last, :status, CAST(:measures AS jsonb))"
            ),
            {
                "model_name": model_name,
                "checked_at": datetime.now(timezone.utc),
                "model_version": int(model_version),
                "first": int(first_scenario),
                "last": int(last_scenario),
                "status": status,
                "measures": json.dumps(measures, default=float),
            },
        )


def check_data_drift(spec):
    # 최근 drift.window건의 입력을 champion 학습 데이터와 비교해 기록하고 판정을 돌려줌. champion이 없거나 입력이 모자라면 None
    model_name = spec["name"]
    champion = fetch_champion_version(model_name)
    if champion is None:
        return None
    version, run_id = champion
    recent = import_function_module(spec["function"], "monitoring.input_drift").measure_recent_drift(spec, run_id)
    if recent is None:
        return None
    first_scenario, last_scenario, measures = recent
    status = classify_data_drift(measures, spec["drift"])
    record_drift_check(model_name, version, first_scenario, last_scenario, status, measures)
    return status


def compare_new_data(spec, data_dir):
    # 학습 전에 불림: 이번 학습 데이터 가운데 지난 학습 뒤 새로 쌓인 데이터가 그 전 데이터와 얼마나 다른지 잼. 비교할 것이 없으면 None
    return import_function_module(spec["function"], "monitoring.input_drift").measure_new_data_drift(spec, data_dir)


def record_champion_comparison(spec, run_name, data_dir):
    # 학습 뒤에 불림: 운영 모델(champion)과 후보 모델(이번 run)의 학습 데이터 차이를 학습 run에 남김 (drift/* 지표, aiems.drift_status 태그)
    model_name = spec["name"]
    champion = fetch_champion_version(model_name)
    if champion is None:
        return
    measures = import_function_module(spec["function"], "monitoring.input_drift").measure_champion_data_drift(spec, champion[1], data_dir)
    client = connect_tracking_server()
    _, run_id = fetch_run_id(client, model_name, run_name)
    for name, value in measures.items():
        client.log_metric(run_id, f"drift/{name}", float(value))
    client.set_tag(run_id, "aiems.drift_status", classify_data_drift(measures, spec["drift"]))
