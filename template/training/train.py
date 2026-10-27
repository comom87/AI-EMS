# [학습 컨테이너] 학습·평가 코드: training/model_train.py가 만든 명령으로 Slurm 작업 안에서 실행됨
# 컨테이너에는 MLFLOW_TRACKING_URI가 들어가므로 MLflow 서버에 바로 기록함. 저장소는 /workspace에 연결되고 그곳에서 실행됨


def fit_model(config_path, data_dir, exp_name, run_name):
    # data_dir의 학습 데이터로 학습하고, MLflow 실험 exp_name에 run 이름 run_name으로 run을 만들어 모델 파일을 올림
    # 올린 파일은 서빙 때 serving/predictor.py의 load_model이 내려받은 폴더로 받음
    raise NotImplementedError


def evaluate_model(config_path, data_dir, exp_name, run_name, model_run_id):
    # model_run_id의 모델 파일을 내려받아 시험 데이터로 평가하고, run 이름 run_name으로 run을 만들어 evaluation.metric 지표를 기록함
    # 후보와 champion을 같은 시험 데이터로 평가하도록, 시험 데이터 고르는 방법은 데이터가 늘어나도 바뀌지 않게 함
    raise NotImplementedError
