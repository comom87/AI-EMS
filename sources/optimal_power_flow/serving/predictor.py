# [서빙] 공용 서빙 앱(serving/app.py)이 부르는 최적 조류 예측 모델의 함수들. 서빙 이미지 안에서만 import됨 (torch 사용)
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import yaml
from gridfm_graphkit.datasets.graph_builder import build_hetero_data
from gridfm_graphkit.io.config_version import resolve_config
from gridfm_graphkit.io.param_handler import NestedNamespace, get_task, get_task_transforms, load_normalizer
from torch_geometric.data import Batch

from common.config.model_specs import load_function_model_spec
from sources.optimal_power_flow.serving import serving_features

PREDICTION_COLUMN_NAMES = {
    "active res. (MW)": "active_res_mw",
    "reactive res. (MVar)": "reactive_res_mvar",
    "PBE": "pbe_mva",
}
# 이 모델이 구독하는 데이터 소스 (기능 폴더 models/model.yaml의 data_source)
DATA_SOURCE = load_function_model_spec(Path(__file__).parents[1].name)["data_source"]
# 예측 결과를 저장하는 이 모델 스키마의 테이블과 정렬 열 (기능 폴더의 storage/schema.sql)
PREDICTION_TABLES = {"bus_predictions": "bus", "gen_predictions": "idx"}


def load_model(model_dir):
    # model_dir: champion 학습 run의 MLflow 파일을 내려받은 폴더
    config_path = next((model_dir / "config").glob("*.yaml"))
    with open(config_path) as f:
        args = NestedNamespace(**resolve_config(yaml.safe_load(f), config_path=str(config_path)))

    normalizer = load_normalizer(args)
    stats = torch.load(model_dir / "stats" / "normalizer_stats.pt", weights_only=True)
    normalizer.fit_from_dict(stats[args.data.networks[0]])
    task = get_task(args, [normalizer])
    task.load_state_dict(torch.load(model_dir / "model" / "best_model_state_dict.pt", map_location="cpu"))
    task.eval()
    return {
        "task": task,
        "normalizer": normalizer,
        "transforms": get_task_transforms(args),
        "sizes": serving_features.fetch_topology_sizes(DATA_SOURCE),
    }


def fetch_latest_scenario(store):
    # 서빙이 예측할 최신 입력: Redis에 올라간 가장 최근 시나리오 번호
    return serving_features.fetch_latest_scenario(store, DATA_SOURCE)


def predict_scenario(model, store, scenario):
    tables = serving_features.fetch_scenario_tables(store, DATA_SOURCE, scenario, model["sizes"])
    data = build_hetero_data(*tables, scenario)
    model["normalizer"].transform(data)
    batch = Batch.from_data_list([model["transforms"](data)])
    with torch.no_grad():
        predictions = model["task"].predict_step(batch, 0)
    bus = pd.DataFrame(predictions["bus"]).rename(columns=PREDICTION_COLUMN_NAMES)
    gen = pd.DataFrame(predictions["gen"])
    gen["idx"] = tables[1].loc[tables[1]["in_service"] == 1, "idx"].to_numpy()
    return {"bus_predictions": bus, "gen_predictions": gen}


def calculate_metrics(predictions):
    # prediction_metrics 테이블에 들어갈 요약 지표. pg_rmse_mw는 model.yaml monitoring.metric_column
    bus, gen = predictions["bus_predictions"], predictions["gen_predictions"]
    return {
        "pg_rmse_mw": float(np.sqrt(np.mean(np.square(gen["p_mw_pred"] - gen["p_mw_target"])))),
        "vm_rmse_pu": float(np.sqrt(np.mean(np.square(bus["Vm_pred"] - bus["Vm_target"])))),
        "mean_active_res_mw": float(bus["active_res_mw"].mean()),
        "mean_reactive_res_mvar": float(bus["reactive_res_mvar"].mean()),
    }


def format_predictions(predictions):
    # /predict 응답에 넣을 값
    return {
        "bus": predictions["bus_predictions"][["bus", "Vm_pred", "Va_pred", "Pg_pred", "Qg_pred"]].to_dict(orient="records"),
        "gen": predictions["gen_predictions"][["idx", "bus", "p_mw_pred"]].to_dict(orient="records"),
    }
