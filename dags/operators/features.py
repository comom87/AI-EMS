# 데이터 소스 파이프라인의 Feast operator: 정의 적용(apply_features), Redis 반영(materialize_features)
# 모든 단계는 (spec, context, **args)를 받음. 자세한 약속은 dags/operators/__init__.py
from airflow.models import Variable

from common.storage.feature_store import (
    apply_feature_definitions,
    calculate_definitions_hash,
    connect_feature_store,
    materialize_source_features,
)

# 마지막으로 적용한 Feast 정의 파일들의 지문을 남겨 두는 Airflow 변수
FEATURE_HASH_VARIABLE = "feature_definitions_hash"


def apply_changed_features(spec, context):
    # 데이터·기능 폴더의 Feast 정의 파일이 지난번 적용 때와 같고 등록 정보도 남아 있으면 건너뜀. 적용했으면 True
    current = calculate_definitions_hash()
    if connect_feature_store().list_feature_views() and Variable.get(FEATURE_HASH_VARIABLE, default_var=None) == current:
        return False
    apply_feature_definitions()
    Variable.set(FEATURE_HASH_VARIABLE, current)
    return True


def materialize_features(spec, context):
    # 데이터 소스에 새로 들어온 데이터를 서빙이 읽는 Redis(Feast 온라인 저장소)에 올림
    materialize_source_features(spec["name"])
