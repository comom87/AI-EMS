# [Feast] 이 기능이 서빙 때 읽는 피처 묶음 (Feast FeatureService)
# 데이터 소스의 피처 뷰(data/<소스>/feature_store.py)에서 필요한 열만 골라 묶음. 이름 앞에 기능 이름을 붙여 다른 기능과 겹치지 않게 함
# 적용: 등록 DAG가 정의 파일이 바뀐 것을 알아채고 common/storage/feature_store.py로 모아 적용
from feast import FeatureService

from data.kpg193.feature_store import bus_view

your_inputs = FeatureService(name="your_function_kpg193_bus", features=[bus_view[["Pd", "Qd"]]])
