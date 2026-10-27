# 어느 파이프라인에서나 쓰는 일반 operator(function, apply_schema)와 다른 operator 파일이 쓰는 도우미
# 모든 단계는 (spec, context, **args)를 받음. 자세한 약속은 dags/operators/__init__.py
from common.config.model_specs import import_function_module
from common.config.source_specs import import_data_source_module
from common.storage.schemas import apply_model_schema, apply_source_schema


def fetch_run_name(context):
    # 한 번의 파이프라인 실행 안에서 학습·평가·정리·등록 단계가 같은 MLflow run 이름을 씀
    return context["dag_run"].start_date.strftime("train_%Y%m%d_%H%M%S")


def import_function_target(spec, target):
    # 모델: "training.dataset_preparation.export_dataset" → sources/<기능>/training/dataset_preparation.py의 export_dataset
    # 데이터 소스: "ingest.register_new_data" → data/<소스>/handler/ingest.py의 register_new_data
    module, name = target.rsplit(".", 1)
    if spec["kind"] == "source":
        return getattr(import_data_source_module(spec["name"], module), name)
    return getattr(import_function_module(spec["function"], module), name)


def run_function(spec, context, function, **args):
    # 기능 폴더(또는 데이터 소스 handler/)의 함수를 (spec, **args)로 부름. 돌려준 값은 다음 단계가 ti.xcom_pull로 받을 수 있음
    return import_function_target(spec, function)(spec, **args)


def apply_owner_schema(spec, context):
    # 새 DB에서도 바로 돌도록 자기 폴더의 schema.sql을 먼저 적용함
    # 데이터 소스: data/<소스>/storage/schema.sql (원본·실시간 표), 모델: sources/<기능>/storage/schema.sql (서빙이 예측을 저장할 표)
    if spec["kind"] == "source":
        apply_source_schema(spec["name"])
    else:
        apply_model_schema(spec["name"])
