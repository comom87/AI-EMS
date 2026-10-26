# 데이터 소스 등록 DAG(<소스>_register): 새 데이터를 등록하고 Feast 온라인 저장소(Redis)에 올림
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

from common.config.source_specs import import_data_source_module
from common.storage.feature_store import (
    apply_feature_definitions,
    calculate_definitions_hash,
    connect_feature_store,
    materialize_source_features,
)
from common.workflows.dag_settings import START_DATE, fetch_registration_dag_id
from common.workflows.datasets import fetch_data_source_dataset

# 등록 DAG 작업을 다시 시도하기 전 기다리는 시간. 등록 주기(15분) 안에 재시도가 끝나도록 짧게 둠
REGISTRATION_RETRY_DELAY = timedelta(minutes=1)
# 마지막으로 적용한 Feast 정의 파일들의 지문을 남겨 두는 Airflow 변수
FEATURE_HASH_VARIABLE = "feature_definitions_hash"


def apply_changed_feature_definitions():
    # Feast 정의 파일이 지난번 적용 때와 같고 등록 정보도 남아 있으면 건너뜀. 적용했으면 True
    current = calculate_definitions_hash()
    if connect_feature_store().list_feature_views() and Variable.get(FEATURE_HASH_VARIABLE, default_var=None) == current:
        return False
    apply_feature_definitions()
    Variable.set(FEATURE_HASH_VARIABLE, current)
    return True


def build_registration_dag(source_spec):
    # 데이터 소스마다 새 데이터를 등록하고 Feast 온라인 저장소(Redis)에 올림. 끝나면 이 소스를 구독하는 모델의 예측 DAG가 실행됨
    # 등록 코드는 data/<소스>/handler/ingest.py의 apply_source_schema(source), register_new_data(source)
    name = source_spec["name"]
    ingest = import_data_source_module(name, "ingest")

    @dag(
        dag_id=fetch_registration_dag_id(name),
        schedule=source_spec["schedule"],
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        default_args={"retries": source_spec["retries"], "retry_delay": REGISTRATION_RETRY_DELAY},
        tags=[name, "data"],
    )
    def run_registration():
        @task
        def apply_schema() -> None:
            # 새 DB에서도 바로 돌도록, 이 소스의 schema.sql을 먼저 적용함
            ingest.apply_source_schema(name)

        @task
        def apply_features() -> bool:
            # 데이터·기능 폴더의 Feast 정의 파일이 바뀌었을 때만 모두 모아 적용함
            return apply_changed_feature_definitions()

        @task
        def register_data():
            return ingest.register_new_data(name)

        @task(outlets=[fetch_data_source_dataset(name)])
        def materialize_features() -> None:
            # 새로 등록한 데이터를 서빙이 읽는 Redis에 올림. 끝나면 이 소스를 구독하는 예측 DAG가 실행됨
            materialize_source_features(name)

        apply_schema() >> apply_features() >> register_data() >> materialize_features()

    return run_registration()
