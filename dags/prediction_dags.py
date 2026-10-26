# Airflow DAG 파일: sources/*/models/model.yaml마다 예측 DAG(<모델>_predict)를 만듭니다.
# Airflow는 "airflow"와 "dag"라는 글자가 모두 있는 파일만 읽으므로 이 주석을 지우지 마세요.
from common.config.model_specs import list_model_names, load_model_spec
from common.workflows.prediction_pipeline import build_prediction_dag

for model_name in list_model_names():
    build_prediction_dag(load_model_spec(model_name))
