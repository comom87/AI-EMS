# [Airflow] 입력 분포 비교: 무엇을 비교할지 정함. 판정과 기록은 common/monitoring/drift.py가 함
# 점검 DAG가 measure_recent_drift, 학습 DAG가 measure_training_drift를 부름. Airflow 안에서 import되므로 torch를 import하지 않음
# measures: {이름: 값}. 이름이 _psi로 끝나는 값(PSI, 0.25 이상이면 분포 변화)과 out_of_range_ratio(학습 범위 밖 비율)는 꼭 넣음
# PSI 계산은 common.monitoring.drift의 compute_psi(reference, current)를 씀


def measure_recent_drift(spec, champion_run_id):
    # champion이 학습한 입력(champion run 태그, training/data.py의 describe_dataset)과 최근 drift.window개 입력을 비교
    # (첫 시나리오, 마지막 시나리오, measures)를 돌려주고, 아직 입력이 모자라면 None
    raise NotImplementedError


def measure_training_drift(spec, champion_run_id, data_dir):
    # 새 학습 데이터(data_dir)를 champion이 학습한 입력과 비교해 measures를 돌려줌 (학습 run에 기록만 하고 승격에는 쓰지 않음)
    raise NotImplementedError
