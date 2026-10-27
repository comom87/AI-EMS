# Airflow DAG 파일: 플랫폼, 데이터 소스, 모델마다 pipelines/<이름>.yaml로 DAG를 조립합니다.
# 플랫폼: common/<영역>/pipelines/ → platform_<이름> (예: platform_slurm_job_records)
# 데이터 소스: data/<소스>/pipelines/ → <소스>_<이름> (예: kpg193_register), 모델: sources/<기능>/pipelines/ → <모델>_<이름> (예: optimal_power_flow_training)
# Airflow는 "airflow"와 "dag"라는 글자가 모두 있는 파일만 읽으므로 이 주석을 지우지 마세요.
from common.config.model_specs import list_model_names, load_model_spec
from common.config.source_specs import list_data_source_names, load_source_spec
from dags.pipeline_builder import PLATFORM_SPEC, build_pipeline_dag, list_pipeline_paths

specs = (
    [PLATFORM_SPEC]
    + [load_source_spec(name) for name in list_data_source_names()]
    + [load_model_spec(name) for name in list_model_names()]
)
for spec in specs:
    for path in list_pipeline_paths(spec):
        build_pipeline_dag(spec, path)
