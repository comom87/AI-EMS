# 모든 DAG가 같이 쓰는 값: 시작일과 DAG 이름 (DAG끼리 서로 실행할 때도 이 이름으로 찾음)
from datetime import datetime

START_DATE = datetime(2026, 1, 1)


def fetch_registration_dag_id(source_name):
    return f"{source_name}_register"


def fetch_training_dag_id(model_name):
    return f"{model_name}_training"


def fetch_prediction_dag_id(model_name):
    return f"{model_name}_predict"


def fetch_monitoring_dag_id(model_name):
    return f"{model_name}_monitoring"
