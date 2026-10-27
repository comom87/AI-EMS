# 파이프라인 YAML의 operator: 이름마다 플랫폼이 제공하는 단계 하나. dags/pipeline_builder.py가 Airflow 작업으로 감쌈
# 모든 단계는 (spec, context, **args)를 받음. spec: 모델({name: <모델>, function: <기능 폴더>, kind: model}), 데이터 소스({name: <소스>}), 또는 플랫폼({name: platform}),
# context: Airflow 실행 정보(dag_run, ti, params), args: YAML의 args (Airflow 템플릿이 먼저 풀림)
# 단계 함수는 쓰이는 곳별 파일에 있고, 여기에는 YAML 이름 → 함수 목록만 둠. 새 operator는 알맞은 파일에 함수를 만들고 여기에 이름을 올림
from dags.operators.features import apply_changed_features, materialize_features
from dags.operators.monitoring import check_drift, check_performance, request_retraining
from dags.operators.general import apply_owner_schema, import_function_target, run_function
from dags.operators.slurm_records import record_job_endings
from dags.operators.training import (
    delete_run_files,
    measure_new_data_drift,
    record_champion_data_drift,
    register_candidate_model,
    submit_evaluate_job,
    submit_train_job,
)

# YAML의 operator 이름 → 단계 함수. trigger_pipeline은 Airflow의 다른 DAG 실행 작업이라 pipeline_builder.py가 직접 만듦
OPERATORS = {
    "function": run_function,
    "slurm_train": submit_train_job,
    "slurm_evaluate": submit_evaluate_job,
    "measure_new_data_drift": measure_new_data_drift,
    "record_champion_data_drift": record_champion_data_drift,
    "delete_run_files": delete_run_files,
    "register_model": register_candidate_model,
    "apply_schema": apply_owner_schema,
    "apply_features": apply_changed_features,
    "materialize_features": materialize_features,
    "check_performance": check_performance,
    "check_drift": check_drift,
    "request_retraining": request_retraining,
    "record_finished_jobs": record_job_endings,
}
# False를 돌려주면 뒤 단계를 건너뛰는 operator
SHORT_CIRCUIT_OPERATORS = {"request_retraining"}
# 기능 폴더의 함수를 가리키는 args 칸. DAG를 만들 때 그 함수가 있는지 미리 확인함
FUNCTION_TARGET_ARGS = ["function", "job", "describe"]
