# [Airflow] 학습 데이터 준비: 실시간 데이터를 graphkit이 읽는 형식으로 내보내고, 검사하고, run에 남길 설명을 만듦
# Airflow 안에서 import되므로 torch를 import하지 않음
import shutil
import subprocess
import sys

import pandas as pd
import yaml
from sqlalchemy import create_engine, text

from common.config.settings import DATABASE_URL, PROJECT_ROOT

SCENARIOS_PER_PARTITION = 200

# 내보낼 실시간 테이블과 정렬 순서 (파일 이름은 datakit 출력과 같게)
EXPORT_TABLES = {
    "bus_data": ("bus_realtime", "scenario, bus"),
    "gen_data": ("gen_realtime", "scenario, idx"),
    "branch_data": ("branch_realtime", "scenario, idx"),
    "y_bus_data": ("y_bus_realtime", "scenario, index1, index2"),
    "runtime_data": ("runtime_realtime", "scenario"),
}

VALIDATION_MODES = {
    "OptimalPowerFlow": "opf",
    "PowerFlow": "pf",
}


def read_config(config_path):
    with open(PROJECT_ROOT / config_path) as f:
        return yaml.safe_load(f)


def fetch_network_name(spec):
    # 학습 설정의 계통 이름 (예: KPG193_ver2_0). 내보낸 데이터 폴더 data_dir/<계통>/raw/의 이름이기도 함
    return read_config(spec["training"]["config"])["data"]["networks"][0]


def export_dataset(spec):
    # 지금까지 들어온 실시간 시나리오 전체를 graphkit이 읽는 datakit 형식(파티션 parquet)으로 내보냄
    engine = create_engine(DATABASE_URL)
    network_dir = PROJECT_ROOT / spec["training"]["data_dir"] / fetch_network_name(spec)
    if network_dir.exists():
        shutil.rmtree(network_dir)
    raw_dir = network_dir / "raw"
    raw_dir.mkdir(parents=True)

    for file_name, (table_name, order) in EXPORT_TABLES.items():
        table = pd.read_sql(f"SELECT * FROM {spec['data_source']}.{table_name} ORDER BY {order}", engine).drop(columns=["event_timestamp"])
        table["scenario_partition"] = table["scenario"] // SCENARIOS_PER_PARTITION
        table.to_parquet(raw_dir / f"{file_name}.parquet", partition_cols=["scenario_partition"], index=False)
        if table_name == "bus_realtime":
            scenario_count = table["scenario"].nunique()

    (raw_dir / "n_scenarios.txt").write_text(str(scenario_count))


def validate_dataset(spec, config_path, data_dir):
    config = read_config(config_path)
    mode = VALIDATION_MODES[config["task"]["task_name"]]
    for network in config["data"]["networks"]:
        raw_dir = PROJECT_ROOT / data_dir / network / "raw"
        subprocess.run(
            [sys.executable, "-m", "gridfm_datakit.cli", "validate", str(raw_dir), "--mode", mode, "--n-partitions", "0"],
            check=True,
        )


def describe_dataset(spec, config_path, data_dir):
    # 학습 run에 남길 태그 (앞부분은 데이터 소스 이름, 예: kpg193.data_dir). 분포 점검이 champion의 태그로 학습 데이터를 다시 찾음
    config = read_config(config_path)
    prefix = spec["data_source"]
    description = {f"{prefix}.data_dir": data_dir, f"{prefix}.config_path": config_path}
    for network in config["data"]["networks"]:
        raw_dir = PROJECT_ROOT / data_dir / network / "raw"
        description[f"{prefix}.{network}.scenario_count"] = (raw_dir / "n_scenarios.txt").read_text().strip()
    if data_dir == spec["training"]["data_dir"]:
        scenario_count = int(description[f"{prefix}.{fetch_network_name(spec)}.scenario_count"])
        last_event = pd.read_sql(
            text(f"SELECT MAX(event_timestamp) FROM {spec['data_source']}.bus_realtime WHERE scenario < :count"),
            create_engine(DATABASE_URL),
            params={"count": scenario_count},
        ).iloc[0, 0]
        description[f"{prefix}.realtime_last_event"] = last_event.isoformat()
    return description
