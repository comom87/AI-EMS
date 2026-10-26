# 점검 DAG(<모델>_monitoring): 예측이 저장될 때마다 성능과 입력 분포를 점검하고, 필요하면 학습 DAG를 실행함
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from common.monitoring.drift import check_data_drift
from common.monitoring.performance import check_model_performance
from common.monitoring.retraining import check_training_availability, record_retraining_request
from common.workflows.dag_settings import START_DATE, fetch_monitoring_dag_id, fetch_training_dag_id
from common.workflows.datasets import fetch_predictions_dataset


def build_monitoring_dag(spec):
    # 예측이 저장될 때마다 성능과 입력 분포를 점검하고, 필요하면 학습 DAG를 실행함 (cooldown_days 안에는 다시 실행하지 않음)
    name = spec["name"]
    training_dag_id = fetch_training_dag_id(name)

    @dag(
        dag_id=fetch_monitoring_dag_id(name),
        schedule=[fetch_predictions_dataset(name)],
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        tags=[name, "monitoring"],
    )
    def run_monitoring():
        @task
        def check_performance() -> bool:
            return check_model_performance(spec)

        @task
        def check_drift() -> str | None:
            return check_data_drift(spec)

        @task.short_circuit
        def request_retraining(degraded: bool, drift_status: str | None) -> bool:
            reasons = []
            if degraded:
                reasons.append("performance_degradation")
            if drift_status in spec["drift"]["retrain_on"]:
                reasons.append(drift_status)
            if not reasons or not check_training_availability(training_dag_id, spec["training"]["cooldown_days"]):
                return False
            record_retraining_request(name, reasons)
            return True

        trigger_training = TriggerDagRunOperator(
            task_id="trigger_training",
            trigger_dag_id=training_dag_id,
            trigger_run_id="retrain_{{ ts_nodash }}",
        )

        request_retraining(check_performance(), check_drift()) >> trigger_training

    return run_monitoring()
