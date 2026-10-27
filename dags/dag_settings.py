# 모든 DAG가 같이 쓰는 값: 시작일과 DAG 이름 (DAG끼리 서로 실행할 때도 이 이름으로 찾음)
from datetime import datetime

START_DATE = datetime(2026, 1, 1)


def fetch_pipeline_dag_id(owner_name, pipeline_name):
    # pipelines/<이름>.yaml → <소스 또는 모델 이름>_<이름> (예: kpg193_register, optimal_power_flow_training)
    return f"{owner_name}_{pipeline_name}"
