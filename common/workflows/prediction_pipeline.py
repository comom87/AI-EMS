# 예측 DAG(<모델>_predict): 데이터 소스에 새 데이터가 들어올 때마다 서빙 앱에 예측을 요청함
import requests
from airflow.decorators import dag, task

from common.config.settings import read_setting
from common.storage.schemas import apply_model_schema
from common.workflows.dag_settings import START_DATE, fetch_prediction_dag_id
from common.workflows.datasets import fetch_data_source_dataset, fetch_predictions_dataset

# 서빙 컨테이너가 예측을 마칠 때까지 기다리는 최대 시간 (초)
PREDICTION_TIMEOUT_SECONDS = 300


def build_prediction_dag(spec):
    # 데이터 소스에 새 데이터가 들어올 때마다 모델 서빙에 예측을 요청함. 예측이 저장되면 <모델>_monitoring이 실행됨
    name = spec["name"]
    url = f"http://localhost:{read_setting('AIEMS_SERVING_PORT')}/models/{name}/predict"

    @dag(
        dag_id=fetch_prediction_dag_id(name),
        schedule=[fetch_data_source_dataset(spec["data_source"])],
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        tags=[name, "serving"],
    )
    def run_prediction():
        @task
        def apply_schema() -> None:
            # 서빙이 예측을 저장할 표(sources/<기능>/schema.sql)를 먼저 만들어 둠
            apply_model_schema(name)

        @task(outlets=[fetch_predictions_dataset(name)])
        def request_prediction() -> None:
            requests.post(url, timeout=PREDICTION_TIMEOUT_SECONDS).raise_for_status()

        apply_schema() >> request_prediction()

    return run_prediction()
