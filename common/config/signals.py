# DAG 사이 신호(Airflow Dataset)의 주소: 서빙 앱이 새 예측을 저장했다는 신호. Airflow(dags/pipeline_builder.py)와 서빙 앱(serving/signals.py)이 같은 주소를 쓰도록 여기 둠


def fetch_predictions_uri(model_name):
    return f"aiems://predictions/{model_name}"
