# [서빙] 최적 조류(OPF) 예측 모델이 서빙 때 Feast 온라인 저장소(Redis)에서 최신 입력을 읽는 함수들. source = 데이터 소스 이름 (예: kpg193)
import pandas as pd
from sqlalchemy import create_engine, text

from common.config.settings import DATABASE_URL
from sources.optimal_power_flow.feature_store import fetch_input_service_name


def fetch_topology_sizes(source):
    # 계통의 모선·발전기·선로 수 (모든 시나리오가 같음)
    engine = create_engine(DATABASE_URL)
    return {
        table: int(pd.read_sql(f"SELECT COUNT(*) FROM {source}.{table} WHERE scenario = 0", engine).iloc[0, 0])
        for table in ["bus", "gen", "branch"]
    }


def fetch_latest_scenario(store, source):
    # Redis에 올라간 시나리오 중 가장 최근 것
    materialized_until = store.get_feature_view(f"{source}_bus", allow_registry_cache=False).most_recent_end_time
    return int(
        pd.read_sql(
            text(f"SELECT MAX(scenario) FROM {source}.bus_realtime WHERE event_timestamp <= :until"),
            create_engine(DATABASE_URL),
            params={"until": materialized_until},
        ).iloc[0, 0]
    )


def fetch_input_table(store, source, table, entity_rows):
    # 이 기능의 피처 묶음(sources/optimal_power_flow/feature_store.py)으로 표 하나를 Redis에서 읽음
    service = store.get_feature_service(fetch_input_service_name(source, table))
    return store.get_online_features(features=service, entity_rows=entity_rows).to_df()


def fetch_scenario_tables(store, source, scenario, sizes):
    bus = fetch_input_table(store, source, "bus", [{"scenario": scenario, "bus": bus} for bus in range(sizes["bus"])])
    gen = fetch_input_table(store, source, "gen", [{"scenario": scenario, "idx": idx} for idx in range(sizes["gen"])])
    branch = fetch_input_table(store, source, "branch", [{"scenario": scenario, "idx": idx} for idx in range(sizes["branch"])])

    gen_q_limits = gen.groupby("bus")[["min_q_mvar", "max_q_mvar"]].sum().reset_index()
    bus = bus.merge(gen_q_limits, on="bus", how="left").fillna({"min_q_mvar": 0, "max_q_mvar": 0})
    return bus, gen, branch
