# 모델 점검 파이프라인의 operator: 성능·입력 분포 점검, 재학습 판단 (재학습해도 되는지는 Airflow 실행 기록으로 확인)
# 모든 단계는 (spec, context, **args)를 받음. 자세한 약속은 dags/operators/__init__.py
from datetime import datetime, timedelta, timezone

from airflow.models import DagRun
from airflow.utils.state import DagRunState

from common.monitoring.drift import check_data_drift
from common.monitoring.performance import check_model_performance
from common.monitoring.retraining import record_retraining_request
from dags.dag_settings import fetch_pipeline_dag_id


def check_performance(spec, context, metric_column, aggregation, window, tolerance):
    return check_model_performance(spec["name"], metric_column, aggregation, window, tolerance)


def check_drift(spec, context, window, psi_threshold, out_of_range_threshold):
    return check_data_drift(spec, window, psi_threshold, out_of_range_threshold)


def check_training_availability(training_dag_id, cooldown_days):
    # 학습이 돌고 있거나 cooldown_days 안에 시작한 학습이 있으면 False
    cutoff = datetime.now(timezone.utc) - timedelta(days=cooldown_days)
    for run in DagRun.find(dag_id=training_dag_id):
        if run.state in (DagRunState.QUEUED, DagRunState.RUNNING):
            return False
        if run.start_date is not None and run.start_date > cutoff:
            return False
    return True


def request_retraining(
    spec, context, retrain_on, cooldown_days, pipeline="training", performance_task="check_performance", drift_task="check_drift"
):
    # 성능 저하나 retrain_on에 든 분포 판정이 있고, 학습 파이프라인이 쉬는 중이며 마지막 학습 뒤 cooldown_days가 지났으면 True (뒤 단계가 실행됨)
    ti = context["ti"]
    reasons = []
    if ti.xcom_pull(task_ids=performance_task):
        reasons.append("performance_degradation")
    drift_status = ti.xcom_pull(task_ids=drift_task)
    if drift_status in retrain_on:
        reasons.append(drift_status)
    training_dag_id = fetch_pipeline_dag_id(spec["name"], pipeline)
    if not reasons or not check_training_availability(training_dag_id, cooldown_days):
        return False
    record_retraining_request(spec["name"], reasons)
    return True
