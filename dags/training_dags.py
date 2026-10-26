# Airflow DAG 파일: sources/*/models/model.yaml마다 학습 DAG(<모델>_training)를 만듭니다.
# Airflow는 "airflow"와 "dag"라는 글자가 모두 있는 파일만 읽으므로 이 주석을 지우지 마세요.
from common.config.model_specs import list_model_names, load_model_spec
from common.workflows.training_pipeline import build_training_dag

for model_name in list_model_names():
    build_training_dag(load_model_spec(model_name))
