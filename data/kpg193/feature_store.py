# 데이터 소스 kpg193이 제공하는 Feast 엔티티·피처 뷰 (실시간 테이블을 그대로 피처로)
# Feast 프로젝트는 플랫폼에 하나(infra/feast)라서, 다른 소스와 겹치지 않게 이름 앞에 kpg193_을 붙임
# 적용: common/storage/feature_store.py의 apply_feature_definitions(), Redis 반영: 등록 DAG
from feast import Entity, FeatureView, Field
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import PostgreSQLSource
from feast.types import Float64, Int64
from feast.value_type import ValueType

scenario = Entity(name="kpg193_scenario", join_keys=["scenario"], value_type=ValueType.INT64)
bus = Entity(name="kpg193_bus", join_keys=["bus"], value_type=ValueType.INT64)
idx = Entity(name="kpg193_idx", join_keys=["idx"], value_type=ValueType.INT64)
index1 = Entity(name="kpg193_index1", join_keys=["index1"], value_type=ValueType.INT64)
index2 = Entity(name="kpg193_index2", join_keys=["index2"], value_type=ValueType.INT64)
load = Entity(name="kpg193_load", join_keys=["load"], value_type=ValueType.INT64)

bus_source = PostgreSQLSource(name="kpg193_bus_realtime", table="kpg193.bus_realtime", timestamp_field="event_timestamp")
bus_view = FeatureView(
    name="kpg193_bus",
    entities=[scenario, bus],
    source=bus_source,
    schema=[
        Field(name="load_scenario_idx", dtype=Int64),
        Field(name="Pd", dtype=Float64),
        Field(name="Qd", dtype=Float64),
        Field(name="Pg", dtype=Float64),
        Field(name="Qg", dtype=Float64),
        Field(name="Vm", dtype=Float64),
        Field(name="Va", dtype=Float64),
        Field(name="PQ", dtype=Int64),
        Field(name="PV", dtype=Int64),
        Field(name="REF", dtype=Int64),
        Field(name="vn_kv", dtype=Float64),
        Field(name="min_vm_pu", dtype=Float64),
        Field(name="max_vm_pu", dtype=Float64),
        Field(name="GS", dtype=Float64),
        Field(name="BS", dtype=Float64),
        Field(name="Va_dc", dtype=Float64),
        Field(name="Pg_dc", dtype=Float64),
    ],
)

gen_source = PostgreSQLSource(name="kpg193_gen_realtime", table="kpg193.gen_realtime", timestamp_field="event_timestamp")
gen_view = FeatureView(
    name="kpg193_gen",
    entities=[scenario, idx],
    source=gen_source,
    schema=[
        Field(name="load_scenario_idx", dtype=Int64),
        Field(name="bus", dtype=Int64),
        Field(name="p_mw", dtype=Float64),
        Field(name="q_mvar", dtype=Float64),
        Field(name="min_p_mw", dtype=Float64),
        Field(name="max_p_mw", dtype=Float64),
        Field(name="min_q_mvar", dtype=Float64),
        Field(name="max_q_mvar", dtype=Float64),
        Field(name="cp0_eur", dtype=Float64),
        Field(name="cp1_eur_per_mw", dtype=Float64),
        Field(name="cp2_eur_per_mw2", dtype=Float64),
        Field(name="in_service", dtype=Int64),
        Field(name="is_slack_gen", dtype=Int64),
        Field(name="p_mw_dc", dtype=Float64),
    ],
)

branch_source = PostgreSQLSource(name="kpg193_branch_realtime", table="kpg193.branch_realtime", timestamp_field="event_timestamp")
branch_view = FeatureView(
    name="kpg193_branch",
    entities=[scenario, idx],
    source=branch_source,
    schema=[
        Field(name="load_scenario_idx", dtype=Int64),
        Field(name="from_bus", dtype=Int64),
        Field(name="to_bus", dtype=Int64),
        Field(name="pf", dtype=Float64),
        Field(name="qf", dtype=Float64),
        Field(name="pt", dtype=Float64),
        Field(name="qt", dtype=Float64),
        Field(name="r", dtype=Float64),
        Field(name="x", dtype=Float64),
        Field(name="b", dtype=Float64),
        Field(name="Yff_r", dtype=Float64),
        Field(name="Yff_i", dtype=Float64),
        Field(name="Yft_r", dtype=Float64),
        Field(name="Yft_i", dtype=Float64),
        Field(name="Ytf_r", dtype=Float64),
        Field(name="Ytf_i", dtype=Float64),
        Field(name="Ytt_r", dtype=Float64),
        Field(name="Ytt_i", dtype=Float64),
        Field(name="tap", dtype=Float64),
        Field(name="shift", dtype=Float64),
        Field(name="ang_min", dtype=Float64),
        Field(name="ang_max", dtype=Float64),
        Field(name="rate_a", dtype=Float64),
        Field(name="br_status", dtype=Int64),
        Field(name="pf_dc", dtype=Float64),
        Field(name="pt_dc", dtype=Float64),
    ],
)

y_bus_source = PostgreSQLSource(name="kpg193_y_bus_realtime", table="kpg193.y_bus_realtime", timestamp_field="event_timestamp")
y_bus_view = FeatureView(
    name="kpg193_y_bus",
    entities=[scenario, index1, index2],
    source=y_bus_source,
    schema=[
        Field(name="load_scenario_idx", dtype=Int64),
        Field(name="G", dtype=Float64),
        Field(name="B", dtype=Float64),
    ],
)

load_profile_source = PostgreSQLSource(
    name="kpg193_load_profile_realtime",
    table="kpg193.scenarios_agg_load_profile_realtime",
    timestamp_field="event_timestamp",
)
load_profile_view = FeatureView(
    name="kpg193_load_profile",
    entities=[scenario, load],
    source=load_profile_source,
    schema=[
        Field(name="load_scenario", dtype=Int64),
        Field(name="p_mw", dtype=Float64),
        Field(name="q_mvar", dtype=Float64),
    ],
)

runtime_source = PostgreSQLSource(name="kpg193_runtime_realtime", table="kpg193.runtime_realtime", timestamp_field="event_timestamp")
runtime_view = FeatureView(
    name="kpg193_runtime",
    entities=[scenario],
    source=runtime_source,
    schema=[
        Field(name="load_scenario_idx", dtype=Int64),
        Field(name="ac", dtype=Float64),
        Field(name="dc", dtype=Float64),
    ],
)
