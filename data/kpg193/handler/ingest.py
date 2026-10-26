# datakit 형식 OPF 시나리오 데이터의 등록 함수. source = 소스 이름 = DB 스키마 이름 (예: kpg193)
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

from common.storage.schemas import apply_sql_file
from common.config.settings import DATABASE_URL
from common.config.source_specs import fetch_data_source_dir

EXCLUDED_FILES = {"stats"}

TABLE_NAME_OVERRIDES = {
    "bus_data": "bus",
    "gen_data": "gen",
    "branch_data": "branch",
    "y_bus_data": "y_bus",
    "runtime_data": "runtime",
}

RAW_TABLES = ["bus", "gen", "branch", "y_bus", "runtime"]
LOAD_PROFILE_TABLE = "scenarios_agg_load_profile"


def apply_source_schema(source):
    # data/<소스>/schema.sql: 스키마를 만들고, 원본·실시간 테이블이 있으면 인덱스까지 만듦. 등록 DAG가 시작할 때마다 부름
    apply_sql_file(fetch_data_source_dir(source) / "schema.sql")


def register_dataset(source, data_dir):
    # datakit으로 만든 원본 시나리오(parquet)를 <소스> 스키마의 원본 테이블로 등록
    engine = create_engine(DATABASE_URL)
    for path in Path(data_dir).iterdir():
        if path.suffix != ".parquet" or path.stem in EXCLUDED_FILES:
            continue
        table_name = TABLE_NAME_OVERRIDES.get(path.stem, path.stem)
        pd.read_parquet(path).to_sql(table_name, engine, schema=source, if_exists="replace", index=False)
    # 테이블을 통째로 바꾸면 인덱스도 사라지므로 다시 만듦
    apply_source_schema(source)


def fetch_total_scenario_count(engine, source):
    with engine.connect() as connection:
        return connection.execute(text(f"SELECT COUNT(DISTINCT scenario) FROM {source}.bus")).scalar()


def fetch_next_scenario_index(engine, source):
    with engine.connect() as connection:
        table_exists = connection.execute(text(f"SELECT to_regclass('{source}.bus_realtime') IS NOT NULL")).scalar()
        if not table_exists:
            return 0
        return connection.execute(text(f"SELECT COUNT(DISTINCT scenario) FROM {source}.bus_realtime")).scalar()


def fetch_load_scenario_index(engine, source, physical_scenario_index):
    with engine.connect() as connection:
        return connection.execute(
            text(f"SELECT load_scenario_idx FROM {source}.bus WHERE scenario = :scenario LIMIT 1"),
            {"scenario": physical_scenario_index},
        ).scalar()


def fetch_scenario_tables(engine, source, scenario_index):
    # 원본 시나리오를 다 쓰면 처음부터 다시 순환함. 실시간 번호(scenario_index)는 계속 늘어남
    physical_scenario_index = scenario_index % fetch_total_scenario_count(engine, source)
    tables = {}
    for table_name in RAW_TABLES:
        table = pd.read_sql(
            text(f"SELECT * FROM {source}.{table_name} WHERE scenario = :scenario"),
            engine,
            params={"scenario": physical_scenario_index},
        )
        table["scenario"] = scenario_index
        tables[f"{table_name}_realtime"] = table

    load_profile = pd.read_sql(
        text(f"SELECT * FROM {source}.{LOAD_PROFILE_TABLE} WHERE load_scenario = :load_scenario"),
        engine,
        params={"load_scenario": fetch_load_scenario_index(engine, source, physical_scenario_index)},
    )
    load_profile["scenario"] = scenario_index
    tables[f"{LOAD_PROFILE_TABLE}_realtime"] = load_profile
    return tables


def record_realtime_scenario(engine, source, tables, event_timestamp):
    for table_name, table in tables.items():
        table.assign(event_timestamp=event_timestamp).to_sql(
            table_name, engine, schema=source, if_exists="append", index=False
        )


def register_new_data(source):
    # 등록 DAG가 부름. 실제 계통 데이터가 들어오는 상황을 흉내 내어, 원본 시나리오를 다음 번호로 하나씩 실시간 테이블에 등록함
    engine = create_engine(DATABASE_URL)
    scenario_index = fetch_next_scenario_index(engine, source)
    record_realtime_scenario(engine, source, fetch_scenario_tables(engine, source, scenario_index), datetime.now(timezone.utc))
    if scenario_index == 0:
        # 실시간 테이블이 방금 처음 만들어졌으므로 인덱스를 검
        apply_source_schema(source)
    return scenario_index
