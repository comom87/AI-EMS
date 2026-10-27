# 서빙 앱이 새 예측을 저장했을 때 Airflow에 "예측 저장됨" 신호(dataset 이벤트)를 보내 <모델>_monitoring DAG를 실행함
# 점검 DAG가 도는 동안 쌓인 신호는 Airflow가 다음 한 번의 실행으로 합침
import requests

from common.config.settings import read_setting
from common.config.signals import fetch_predictions_uri

# Airflow가 응답하지 않아도 예측 응답이 오래 늦어지지 않게 짧게 기다림 (초)
SIGNAL_TIMEOUT_SECONDS = 5


def send_predictions_signal(model_name):
    response = requests.post(
        f"http://localhost:{read_setting('AIEMS_AIRFLOW_PORT')}/api/v1/datasets/events",
        json={"dataset_uri": fetch_predictions_uri(model_name)},
        auth=(read_setting("AIEMS_AIRFLOW_API_USER"), read_setting("AIEMS_AIRFLOW_API_PASSWORD")),
        timeout=SIGNAL_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
