# MLflow 서버의 run 찾기·태그·파일 올리기·내려받기
from pathlib import Path

import mlflow

from common.config.settings import MLFLOW_TRACKING_URI, PROJECT_ROOT


def connect_tracking_server():
    return mlflow.MlflowClient(MLFLOW_TRACKING_URI)


def fetch_run_id(client, exp_name, run_name):
    experiment = client.get_experiment_by_name(exp_name)
    run = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string=f"tags.mlflow.runName = '{run_name}'",
    )[0]
    return experiment.experiment_id, run.info.run_id


def fetch_latest_tagged_run_id(exp_name, tag_key):
    # tag_key 태그가 붙은 가장 최근 run. 데이터 설명 태그(describe_dataset)는 학습이 성공한 run에만 붙으므로 지난 학습 run을 찾을 때 씀. 없으면 None
    client = connect_tracking_server()
    experiment = client.get_experiment_by_name(exp_name)
    if experiment is None:
        return None
    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string=f"tags.`{tag_key}` LIKE '%'",
        order_by=["attributes.start_time DESC"],
        max_results=1,
    )
    return runs[0].info.run_id if runs else None


def fetch_run_tags(run_id):
    return connect_tracking_server().get_run(run_id).data.tags


def tag_run(exp_name, run_name, tags):
    client = connect_tracking_server()
    _, run_id = fetch_run_id(client, exp_name, run_name)
    for key, value in tags.items():
        client.set_tag(run_id, key, value)


def upload_run_file(exp_name, run_name, path, artifact_dir):
    client = connect_tracking_server()
    _, run_id = fetch_run_id(client, exp_name, run_name)
    client.log_artifact(run_id, str(PROJECT_ROOT / path), artifact_dir)


def download_run_artifacts(run_id, download_dir):
    download_dir = Path(download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)
    mlflow.artifacts.download_artifacts(run_id=run_id, dst_path=str(download_dir), tracking_uri=MLFLOW_TRACKING_URI)
    return download_dir

