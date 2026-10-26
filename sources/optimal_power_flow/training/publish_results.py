# [학습 컨테이너] 학습·평가 명령(training/jobs.py의 build_publish_command)이 graphkit 실행 뒤에 부름
import re
import shutil
from pathlib import Path

import pandas as pd

from common.model_registry.local_artifacts import LOCAL_RUNS_DIR
from common.model_registry.tracking import connect_tracking_server, fetch_run_id

# graphkit evaluate --compute_dc_ac_metrics가 남기는 DC·AC 기준선 지표 파일
BASELINE_FILE_PATTERN = re.compile(r"(.+)_(?:opf|pf)_ac_dc_metrics\.csv")


def normalize_metric_name(name):
    return re.sub(r"[^0-9a-z]+", "_", name.lower()).strip("_")


def log_test_metrics(client, run_id, artifacts_dir):
    # graphkit이 CSV로만 남기는 시험 지표를 MLflow 지표로 옮김 (예: test/KPG193_ver2_0/rmse_pg_generators_mw)
    test_dir = Path(artifacts_dir) / "test"
    for path in test_dir.glob("*_metrics.csv"):
        baseline = BASELINE_FILE_PATTERN.fullmatch(path.name)
        prefix = f"baseline/{baseline.group(1)}" if baseline else f"test/{path.name.removesuffix('_metrics.csv')}"
        for metric, value in pd.read_csv(path).itertuples(index=False):
            client.log_metric(run_id, f"{prefix}/{normalize_metric_name(metric)}", value)
    for path in test_dir.glob("*_RMSE.csv"):
        network = path.name.removesuffix("_RMSE.csv")
        table = pd.read_csv(path).set_index("Metric")
        for row, values in table.iterrows():
            for column, value in values.items():
                key = f"{normalize_metric_name(row)}_{normalize_metric_name(column)}"
                client.log_metric(run_id, f"test/{network}/{key}", value)


def publish_run_results(exp_name, run_name):
    # graphkit은 run 파일을 로컬 mlruns/에 쓰므로, 지표와 파일을 MLflow 서버로 올리고 로컬 사본을 지움
    client = connect_tracking_server()
    experiment_id, run_id = fetch_run_id(client, exp_name, run_name)
    local_run_dir = LOCAL_RUNS_DIR / experiment_id / run_id
    log_test_metrics(client, run_id, local_run_dir / "artifacts")
    client.log_artifacts(run_id, str(local_run_dir / "artifacts"))
    shutil.rmtree(local_run_dir)
