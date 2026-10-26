# [서빙] 공용 서빙 앱(serving/app.py)이 부르는 함수들. 서빙 이미지 안에서만 import됨
# 아래 이름은 모두 있어야 함. 쓰는 라이브러리는 공용 서빙 이미지(infra/docker/serving/Dockerfile)에 있어야 하므로 운영자에게 요청
# 입력 읽기가 길어지면 serving/inputs.py로 나눔
from pathlib import Path

from common.config.model_specs import load_function_model_spec

# 이 모델이 구독하는 데이터 소스 (models/model.yaml의 data_source)
DATA_SOURCE = load_function_model_spec(Path(__file__).parents[1].name)["data_source"]
# 예측값을 저장할 표와 정렬 열 (schema.sql의 예측값 표). predict_scenario가 돌려주는 dict의 키와 같아야 함
PREDICTION_TABLES = {"your_predictions": "your_key"}


def load_model(model_dir):
    # model_dir: champion 학습 run의 MLflow 파일을 내려받은 폴더. 예측에 필요한 것을 dict로 돌려줌 (predict_scenario의 model로 들어감)
    raise NotImplementedError


def fetch_latest_scenario(store):
    # 예측할 최신 입력의 시나리오 번호(정수). store: Feast 연결 (Redis에 올라간 데이터)
    raise NotImplementedError


def predict_scenario(model, store, scenario):
    # 시나리오 하나를 예측해 {PREDICTION_TABLES의 표 이름: DataFrame}으로 돌려줌. DataFrame 열은 schema.sql의 예측값 표 열과 같게
    raise NotImplementedError


def calculate_metrics(predictions):
    # prediction_metrics에 넣을 요약 지표 {열 이름: 값}. model.yaml monitoring.metric_column이 들어 있어야 함
    raise NotImplementedError


def format_predictions(predictions):
    # POST /models/<모델 이름>/predict 응답에 넣을 값 (JSON으로 바꿀 수 있는 dict)
    raise NotImplementedError
