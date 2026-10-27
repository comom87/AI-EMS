# 파이프라인 YAML을 읽어 Airflow DAG를 조립함
# 데이터 소스: data/<소스>/pipelines/<이름>.yaml → <소스>_<이름>, 모델: sources/<기능>/pipelines/<이름>.yaml → <모델>_<이름>,
# 플랫폼: common/<영역>/pipelines/<이름>.yaml → platform_<이름>
# YAML: dag(description, schedule, params, retries)와 tasks(<작업 이름>: operator, args, dependencies, trigger_rule, retries, outlets)
# operator 목록과 각 단계의 일은 dags/operators/
from datetime import timedelta
from functools import partial

import jinja2
import yaml
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator, ShortCircuitOperator, get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from common.config.model_specs import fetch_model_dir
from common.config.settings import PROJECT_ROOT
from common.config.signals import fetch_predictions_uri
from common.config.source_specs import fetch_data_source_dir
from dags.dag_settings import START_DATE, fetch_pipeline_dag_id
from dags.operators import (
    FUNCTION_TARGET_ARGS,
    OPERATORS,
    SHORT_CIRCUIT_OPERATORS,
    import_function_target,
)

PIPELINES_DIR = "pipelines"
TASK_FIELDS = {"operator", "args", "dependencies", "trigger_rule", "retries", "outlets"}
# Airflow가 만드는 작업이라 OPERATORS에 없는 operator
TRIGGER_OPERATOR = "trigger_pipeline"
RETRY_DELAY = timedelta(minutes=1)
# 데이터 소스·모델과 무관한 플랫폼 일의 주인. 파이프라인은 common/<영역>/pipelines/에 둠 (예: common/slurm/pipelines/)
PLATFORM_SPEC = {"name": "platform", "kind": "platform"}
COMMON_DIR = PROJECT_ROOT / "common"


def list_pipeline_paths(spec):
    # pipelines/*.yaml. 파일 하나가 DAG 하나
    # 데이터 소스는 data/<소스>/, 모델은 sources/<기능>/, 플랫폼은 common/의 모든 영역 폴더
    if spec["kind"] == "platform":
        return sorted(COMMON_DIR.glob(f"*/{PIPELINES_DIR}/*.yaml"))
    if spec["kind"] == "source":
        return sorted((fetch_data_source_dir(spec["name"]) / PIPELINES_DIR).glob("*.yaml"))
    return sorted((fetch_model_dir(spec["name"]) / PIPELINES_DIR).glob("*.yaml"))


def load_pipeline(path):
    with open(path) as f:
        return yaml.safe_load(f)


def validate_pipeline(spec, pipeline, label):
    # DAG를 만들기 전에 YAML 실수를 막음. 오류는 Airflow 불러오기 오류로 보임
    if "tasks" not in pipeline or not pipeline["tasks"]:
        raise ValueError(f"{label} has no tasks")
    for task_id, task in pipeline["tasks"].items():
        unknown = set(task) - TASK_FIELDS
        if unknown:
            raise ValueError(f"{label} task {task_id} has unknown fields {sorted(unknown)}")
        operator = task.get("operator")
        if operator not in OPERATORS and operator != TRIGGER_OPERATOR:
            raise ValueError(f"{label} task {task_id} uses unknown operator {operator}")
        for dependency in task.get("dependencies", []):
            if dependency not in pipeline["tasks"]:
                raise ValueError(f"{label} task {task_id} depends on unknown task {dependency}")
        for name in FUNCTION_TARGET_ARGS:
            if name in task.get("args", {}):
                import_function_target(spec, task["args"][name])


def render_params(spec, params):
    # params 기본값에는 모델 명세를 쓸 수 있음 (예: "{{ spec.training.config }}"). DAG를 만들 때 한 번 풂
    return {
        name: jinja2.Template(value).render(spec=spec) if isinstance(value, str) else value
        for name, value in (params or {}).items()
    }


def resolve_schedule(spec, schedule):
    # predictions: 서빙 앱이 이 모델의 새 예측을 저장할 때마다, 그 밖에는 cron·프리셋 그대로
    # 신호는 Airflow Dataset이고 주소는 common/config/signals.py (서빙 앱도 같은 주소로 신호를 보냄)
    if schedule == "predictions":
        return [Dataset(fetch_predictions_uri(spec["name"]))]
    return schedule


def resolve_outlets(spec, outlets):
    # predictions: "이 모델의 예측이 저장됨" 신호 (schedule: predictions인 파이프라인이 실행됨). 모델 파이프라인에서만 씀
    signals = {"predictions": Dataset(fetch_predictions_uri(spec["name"]))} if spec["kind"] == "model" else {}
    unknown = set(outlets or []) - set(signals)
    if unknown:
        raise ValueError(f"{spec['name']} cannot use outlets {sorted(unknown)}; only model pipelines can use predictions")
    return [signals[outlet] for outlet in outlets or []]


def run_operator(operator, spec, args):
    return OPERATORS[operator](spec, get_current_context(), **args)


def build_pipeline_task(spec, task_id, task):
    options = {
        "task_id": task_id,
        "outlets": resolve_outlets(spec, task.get("outlets")),
        "trigger_rule": task.get("trigger_rule", "all_success"),
    }
    if "retries" in task:
        options["retries"] = task["retries"]
    operator = task["operator"]
    args = task.get("args", {})
    if operator == TRIGGER_OPERATOR:
        return TriggerDagRunOperator(
            trigger_dag_id=fetch_pipeline_dag_id(spec["name"], args["pipeline"]),
            trigger_run_id=args.get("run_id"),
            **options,
        )
    operator_class = ShortCircuitOperator if operator in SHORT_CIRCUIT_OPERATORS else PythonOperator
    return operator_class(python_callable=partial(run_operator, operator, spec), op_kwargs={"args": args}, **options)


def build_pipeline_dag(spec, path):
    pipeline_name = path.stem
    label = str(path.relative_to(PROJECT_ROOT))
    pipeline = load_pipeline(path)
    validate_pipeline(spec, pipeline, label)
    settings = pipeline.get("dag", {})
    dag = DAG(
        dag_id=fetch_pipeline_dag_id(spec["name"], pipeline_name),
        description=settings.get("description"),
        schedule=resolve_schedule(spec, settings.get("schedule")),
        params=render_params(spec, settings.get("params")),
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        # 템플릿을 푼 값이 문자열로 바뀌지 않게 함 (앞 단계가 돌려준 None·숫자를 그대로 받음)
        render_template_as_native_obj=True,
        # owner: Airflow 화면의 Owner 칸. 모델은 따로 적지 않으면 model.yaml의 owner
        default_args={
            "owner": settings.get("owner", spec.get("owner", "airflow")),
            "retries": settings.get("retries", 0),
            "retry_delay": RETRY_DELAY,
        },
        tags=[spec["name"], pipeline_name],
    )
    with dag:
        tasks = {task_id: build_pipeline_task(spec, task_id, task) for task_id, task in pipeline["tasks"].items()}
    for task_id, task in pipeline["tasks"].items():
        for dependency in task.get("dependencies", []):
            tasks[dependency] >> tasks[task_id]
    return dag
