# Feast 프로젝트 하나에 데이터 폴더와 기능 폴더의 정의 파일을 모아 적용하고, 데이터 소스별로 Redis에 반영함
# 설정: infra/feast/feature_store.yaml. 정의: data/<소스>/storage/feature_store.py, sources/<기능>/storage/feature_store.py
import hashlib
import importlib
from datetime import datetime, timezone

from feast import Entity, FeatureService, FeatureStore, FeatureView
from feast.data_source import DataSource

from common.config.settings import PROJECT_ROOT
from common.config.model_specs import FUNCTIONS_DIR
from common.config.source_specs import DATA_DIR

FEAST_DIR = PROJECT_ROOT / "infra" / "feast"
# 데이터 소스·기능 폴더 안에서 Feast 정의 파일의 위치
DEFINITION_FILE = "storage/feature_store.py"
FEAST_OBJECT_TYPES = (Entity, DataSource, FeatureView, FeatureService)


def connect_feature_store():
    return FeatureStore(repo_path=str(FEAST_DIR))


def list_definition_paths():
    # 데이터 정의를 먼저 불러옴 (기능 정의가 데이터의 피처 뷰를 import해서 씀)
    return sorted(DATA_DIR.glob(f"*/{DEFINITION_FILE}")) + sorted(FUNCTIONS_DIR.glob(f"*/{DEFINITION_FILE}"))


def calculate_definitions_hash():
    # 설정과 정의 파일들의 내용 지문. 파일이 바뀌거나 추가·삭제되면 값이 달라짐
    digest = hashlib.sha256()
    for path in [FEAST_DIR / "feature_store.yaml", *list_definition_paths()]:
        digest.update(str(path.relative_to(PROJECT_ROOT)).encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def list_definition_modules():
    return [".".join(path.relative_to(PROJECT_ROOT).with_suffix("").parts) for path in list_definition_paths()]


def collect_feast_objects(module_name):
    module = importlib.import_module(module_name)
    return [value for value in vars(module).values() if isinstance(value, FEAST_OBJECT_TYPES)]


def list_registered_objects(store):
    return (
        store.list_entities()
        + store.list_data_sources()
        + store.list_feature_views()
        + store.list_feature_services()
    )


def collect_all_feast_objects():
    # 기능 정의가 import한 데이터 쪽 객체는 두 번 나오므로 종류·이름으로 하나만 남김
    objects = {}
    for module_name in list_definition_modules():
        for item in collect_feast_objects(module_name):
            objects[(type(item).__name__, item.name)] = item
    return objects


def apply_feature_definitions():
    # 정의 파일에서 사라진 것은 등록에서도 지움 (feast apply 명령과 같은 결과)
    store = connect_feature_store()
    objects = collect_all_feast_objects()
    stale = [item for item in list_registered_objects(store) if (type(item).__name__, item.name) not in objects]
    store.apply(list(objects.values()), objects_to_delete=stale, partial=False)


def materialize_source_features(source):
    # 데이터 소스 하나의 피처 뷰만 Redis에 반영 (지난번 반영 이후 들어온 데이터)
    module = importlib.import_module(f"data.{source}.storage.feature_store")
    views = [item.name for item in vars(module).values() if isinstance(item, FeatureView)]
    connect_feature_store().materialize_incremental(datetime.now(timezone.utc), feature_views=views)
