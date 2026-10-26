from airflow.datasets import Dataset


def fetch_data_source_dataset(data_source):
    # 데이터 소스에 새 데이터가 들어왔다는 Airflow 신호. 그 데이터를 쓰는 모델의 <모델>_predict DAG가 구독함
    return Dataset(f"aiems://data/{data_source}")


def fetch_predictions_dataset(model_name):
    # 모델이 새 예측을 저장했다는 신호. <모델>_monitoring DAG가 구독함
    return Dataset(f"aiems://predictions/{model_name}")
