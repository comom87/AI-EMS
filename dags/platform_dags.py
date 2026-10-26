# Airflow DAG 파일: 모델과 무관한 플랫폼 정리 작업
from airflow.decorators import dag, task

from common.workflows.dag_settings import START_DATE
from common.slurm.job_records import record_finished_jobs


@dag(
    dag_id="platform_slurm_job_records",
    schedule="*/15 * * * *",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    tags=["platform"],
)
def run_slurm_job_records():
    @task
    def update_job_records() -> None:
        # 기다리지 않고 제출했거나 비정상 종료한 작업의 종료 상태를 sacct로 채움
        record_finished_jobs()

    update_job_records()


run_slurm_job_records()
