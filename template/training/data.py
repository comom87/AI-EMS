# [Airflow] 학습 데이터 준비: 학습 DAG(<모델 이름>_training)가 학습 전에 차례로 부름
# 내용은 모델마다 자유롭게 쓰고, 함수 이름과 인자만 지킴. Airflow 안에서 import되므로 torch를 import하지 않음
# spec: models/model.yaml 내용 (spec["data_source"], spec["training"]["data_dir"] 등)


def export_dataset(spec):
    # 학습 DAG 첫 단계. 데이터 소스의 DB 표(<소스>.<표>)를 읽어 전처리하고 spec["training"]["data_dir"]에 저장
    # 저장 위치는 data/<소스>/files/ 아래로 함 (git에서 빠짐). 다른 모델과 폴더가 겹치지 않게 모델 이름 폴더를 씀
    raise NotImplementedError


def validate_dataset(spec, config_path, data_dir):
    # export_dataset 다음. 학습에 쓸 데이터를 검사하고, 문제가 있으면 예외를 냄 (Slurm 학습 전에 DAG가 멈춤)
    # data_dir은 학습 DAG 실행 때 바꿀 수 있음 (기본값은 spec["training"]["data_dir"])
    raise NotImplementedError


def describe_dataset(spec, config_path, data_dir):
    # 학습이 끝난 뒤 학습 run에 붙일 태그 {이름: 값}. 앞부분은 데이터 소스 이름으로 함 (예: kpg193.row_count)
    # monitoring/input_drift.py가 champion run의 이 태그로 그 모델이 학습한 데이터를 다시 찾음
    raise NotImplementedError
