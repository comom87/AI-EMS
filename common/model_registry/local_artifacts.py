# 학습 중 로컬 디스크에 잠깐 두는 run 파일: graphkit이 MLflow 서버에 올리기 전에 쓰는 mlruns/, 평가하려고 내려받은 work/. 학습 DAG가 끝나면 지움
import shutil

from common.config.settings import PROJECT_ROOT
from common.model_registry.tracking import connect_tracking_server

# 학습 컨테이너가 MLflow run을 먼저 쓰는 로컬 폴더 (컨테이너 안 /workspace/mlruns). sources/<기능>/training/publish_results.py가 서버로 올린 뒤 지움
LOCAL_RUNS_DIR = PROJECT_ROOT / "mlruns"
# 평가하려고 내려받은 모델 파일을 두는 곳
WORK_DIR = PROJECT_ROOT / "work"


def fetch_work_dir(model_name, run_name):
    return WORK_DIR / model_name / run_name


def delete_work_dir(model_name, run_name):
    shutil.rmtree(fetch_work_dir(model_name, run_name), ignore_errors=True)


def delete_local_runs(exp_name, run_name):
    client = connect_tracking_server()
    experiment = client.get_experiment_by_name(exp_name)
    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string=f"tags.mlflow.runName LIKE '{run_name}%'",
    )
    for run in runs:
        shutil.rmtree(LOCAL_RUNS_DIR / experiment.experiment_id / run.info.run_id, ignore_errors=True)
