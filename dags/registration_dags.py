# Airflow DAG 파일: data/<소스>/source.yaml마다 등록 DAG(<소스>_register)를 만듭니다.
# Airflow는 "airflow"와 "dag"라는 글자가 모두 있는 파일만 읽으므로 이 주석을 지우지 마세요.
from common.config.source_specs import list_data_source_names, load_source_spec
from common.workflows.registration_pipeline import build_registration_dag

for source_name in list_data_source_names():
    build_registration_dag(load_source_spec(source_name))
