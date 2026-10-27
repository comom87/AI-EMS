# 모델 학습 파이프라인의 operator: Slurm 학습·평가, 학습 데이터 분포 측정·기록, 임시 파일 정리, 등록·승격
# 모든 단계는 (spec, context, **args)를 받음. 자세한 약속은 dags/operators/__init__.py
from common.model_registry.local_artifacts import delete_local_runs, delete_temp_dir, fetch_temp_dir
from common.model_registry.registry import fetch_champion_version, validate_candidate_model
from common.model_registry.tracking import connect_tracking_server, fetch_run_id, tag_run, upload_run_file
from common.monitoring.drift import compare_new_data, record_champion_comparison
from common.slurm.scheduling import submit_training_job
from dags.operators.general import fetch_run_name, import_function_target


def submit_train_job(spec, context, job, config_path, data_dir, describe=None):
    # job: 기능 폴더의 학습 작업 내용 함수. describe: 학습 run에 붙일 데이터 설명 함수 (분포 점검이 champion 학습 데이터를 찾을 때 씀)
    name = spec["name"]
    run_name = fetch_run_name(context)
    built = import_function_target(spec, job)(spec, config_path, data_dir, name, run_name)
    submit_training_job(**built, job_label=run_name, wait=True)
    if describe is not None:
        tag_run(name, run_name, import_function_target(spec, describe)(spec, config_path, data_dir))
    upload_run_file(name, run_name, config_path, "config")
    return run_name


def submit_evaluate_job(spec, context, job, model, config_path, data_dir):
    # model: candidate(이번 학습 run → <run>_eval) 또는 champion(운영 중인 모델 → <run>_champion_eval).
    # champion은 후보와 같은 시험 데이터로 다시 평가해 공정하게 비교함. champion이 없으면 건너뛰고 None
    name = spec["name"]
    run_name = fetch_run_name(context)
    if model == "candidate":
        version = None
        _, model_run_id = fetch_run_id(connect_tracking_server(), name, run_name)
        eval_run_name = f"{run_name}_eval"
    else:
        champion = fetch_champion_version(name)
        if champion is None:
            return None
        version, model_run_id = champion
        eval_run_name = f"{run_name}_champion_eval"
    built = import_function_target(spec, job)(
        spec, config_path, data_dir, name, eval_run_name, model_run_id, fetch_temp_dir(name, run_name) / model,
    )
    submit_training_job(**built, job_label=eval_run_name, wait=True)
    return version


def measure_new_data_drift(spec, context, data_dir):
    # 지난 학습 뒤 새로 쌓인 데이터의 분포 변화를 학습 전에 잼. 돌려준 값은 XCom에 남음 (학습·승격을 막지 않음)
    return compare_new_data(spec, data_dir)


def record_champion_data_drift(spec, context, data_dir):
    # 운영 모델과 후보 모델의 학습 데이터 차이를 이번 학습 run에 남김 (승격 판단에는 쓰지 않음)
    record_champion_comparison(spec, fetch_run_name(context), data_dir)


def delete_run_files(spec, context):
    # 평가용으로 내려받은 모델 파일과 graphkit이 로컬에 남긴 run 파일을 지움
    delete_temp_dir(spec["name"], fetch_run_name(context))
    delete_local_runs(spec["name"], fetch_run_name(context))


def register_candidate_model(spec, context, candidate_task="evaluate_model", champion_task="evaluate_champion"):
    # 후보 평가가 성공했을 때만 등록함. champion 평가가 실패하면 등록만 하고 승격하지 않은 뒤 실패로 알림
    dag_run = context["dag_run"]
    if dag_run.get_task_instance(candidate_task).state != "success":
        raise RuntimeError("Candidate evaluation did not succeed")
    champion_failed = dag_run.get_task_instance(champion_task).state != "success"
    champion_version = context["ti"].xcom_pull(task_ids=champion_task)
    validate_candidate_model(spec, fetch_run_name(context), champion_version, champion_failed)
    if champion_failed:
        raise RuntimeError("Champion evaluation failed; candidate registered without promotion")
