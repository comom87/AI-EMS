# 모델 학습 DAG(<모델>_training): 데이터 준비 → Slurm 학습·평가 → champion과 비교 → 등록·승격
from airflow.decorators import dag, task
from airflow.models.param import Param

from common.config.model_specs import import_function_module
from common.model_registry.registry import fetch_champion_version, validate_candidate_model
from common.model_registry.local_artifacts import delete_local_runs, delete_work_dir, fetch_work_dir
from common.model_registry.tracking import connect_tracking_server, fetch_run_id, tag_run, upload_run_file
from common.monitoring.drift import record_training_drift
from common.slurm.scheduling import submit_training_job
from common.workflows.dag_settings import START_DATE, fetch_training_dag_id


def build_training_dag(spec):
    name = spec["name"]
    training = spec["training"]
    # 학습 데이터 준비와 Slurm 작업 내용은 모델의 기능 폴더 코드 (sources/<기능>/training/data.py, jobs.py)
    dataset = import_function_module(spec["function"], "training.data")
    jobs = import_function_module(spec["function"], "training.jobs")

    @dag(
        dag_id=fetch_training_dag_id(name),
        schedule=training["schedule"],
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        tags=[name, "training"],
        params={
            "config_path": Param(training["config"], type="string", description="Training config YAML"),
            "data_dir": Param(
                training["data_dir"],
                type="string",
                description="Training data directory. Default is the data the model exports; set another directory to train on it as-is",
            ),
        },
    )
    def run_training_pipeline():
        @task
        def export_dataset() -> None:
            dataset.export_dataset(spec)

        @task
        def validate_dataset(**context) -> None:
            params = context["params"]
            dataset.validate_dataset(spec, params["config_path"], params["data_dir"])

        @task(retries=training["retries"])
        def train_model(**context) -> str:
            params = context["params"]
            run_name = context["dag_run"].start_date.strftime("train_%Y%m%d_%H%M%S")
            job = jobs.build_train_job(spec, params["config_path"], params["data_dir"], name, run_name)
            submit_training_job(**job, job_label=run_name, wait=True)
            tag_run(name, run_name, dataset.describe_dataset(spec, params["config_path"], params["data_dir"]))
            upload_run_file(name, run_name, params["config_path"], "config")
            return run_name

        @task(retries=training["retries"])
        def evaluate_model(run_name: str, **context) -> None:
            params = context["params"]
            _, run_id = fetch_run_id(connect_tracking_server(), name, run_name)
            job = jobs.build_evaluate_job(
                spec, params["config_path"], params["data_dir"], name, f"{run_name}_eval", run_id,
                fetch_work_dir(name, run_name) / "candidate",
            )
            submit_training_job(**job, job_label=f"{run_name}_eval", wait=True)

        @task(retries=training["retries"])
        def evaluate_champion(run_name: str, **context) -> str | None:
            # 지금 운영 중인 모델을 후보와 같은 시험 데이터로 다시 평가해 공정하게 비교함
            params = context["params"]
            champion = fetch_champion_version(name)
            if champion is None:
                return None
            version, champion_run_id = champion
            job = jobs.build_evaluate_job(
                spec, params["config_path"], params["data_dir"], name, f"{run_name}_champion_eval", champion_run_id,
                fetch_work_dir(name, run_name) / "champion",
            )
            submit_training_job(**job, job_label=f"{run_name}_champion_eval", wait=True)
            return version

        @task
        def record_data_drift(run_name: str, **context) -> None:
            # 새 학습 데이터가 champion 학습 데이터와 얼마나 다른지 학습 run에 남김 (승격 판단에는 쓰지 않음)
            record_training_drift(spec, run_name, context["params"]["data_dir"])

        @task(trigger_rule="all_done")
        def delete_run_files(run_name: str | None) -> None:
            if run_name is None:
                return
            delete_work_dir(name, run_name)
            delete_local_runs(name, run_name)

        @task(trigger_rule="all_done")
        def validate_model(run_name: str | None, champion_version: str | None, **context) -> None:
            dag_run = context["dag_run"]
            if dag_run.get_task_instance("evaluate_model").state != "success":
                raise RuntimeError("Candidate evaluation did not succeed")
            champion_failed = dag_run.get_task_instance("evaluate_champion").state != "success"
            validate_candidate_model(spec, run_name, champion_version, champion_failed)
            if champion_failed:
                raise RuntimeError("Champion evaluation failed; candidate registered without promotion")

        run_name = train_model()
        export_dataset() >> validate_dataset() >> run_name
        champion_version = evaluate_champion(run_name)
        evaluate_model(run_name) >> champion_version
        cleanup = delete_run_files(run_name)
        champion_version >> cleanup
        record_data_drift(run_name) >> cleanup
        cleanup >> validate_model(run_name, champion_version)

    return run_training_pipeline()
