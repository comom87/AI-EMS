# [Airflow] 입력 분포 비교: 무엇을 비교할지 정함. 판정과 기록은 common/monitoring/drift.py가 함
# 점검 DAG가 measure_recent_drift, 학습 DAG가 measure_new_data_drift(학습 전)와 measure_champion_data_drift(학습 뒤)를 부름. Airflow 안에서 import되므로 torch를 import하지 않음
# measures: {이름: 값}. 이름이 _psi로 끝나는 값(PSI, 0.25 이상이면 분포 변화)과 out_of_range_ratio(학습 범위 밖 비율)는 꼭 넣음
# PSI 계산은 common.monitoring.drift의 compute_psi(reference, current)를 씀


def measure_recent_drift(spec, champion_run_id, window, psi_threshold):
    # champion이 학습한 입력(champion run 태그, training/dataset_preparation.py의 describe_dataset)과 최근 window개 입력을 비교
    # window·psi_threshold는 점검 파이프라인(pipelines/monitoring.yaml) check_drift의 args. psi_threshold는 모선 등 세부 단위 판정에 쓸 수 있음
    # (첫 시나리오, 마지막 시나리오, measures)를 돌려주고, 아직 입력이 모자라면 None
    raise NotImplementedError


def measure_champion_data_drift(spec, champion_run_id, data_dir, psi_threshold):
    # 후보 모델의 학습 데이터(data_dir)를 champion이 학습한 입력과 비교해 measures를 돌려줌 (학습 run에 기록만 하고 승격에는 쓰지 않음)
    raise NotImplementedError


def measure_new_data_drift(spec, data_dir, psi_threshold):
    # 이번 학습 데이터(data_dir) 가운데 지난 학습 뒤 새로 들어온 데이터를 그 전 데이터와 비교해 measures를 돌려줌 (학습 DAG XCom에만 남음)
    # 지난 학습 run은 common.model_registry.tracking의 fetch_latest_tagged_run_id로 찾음. 비교할 것이 없으면 None
    raise NotImplementedError
