# [학습 컨테이너] 학습·평가 명령(training/model_train.py의 build_split_command)이 graphkit 실행 전에 부름
import pandas as pd
import torch
import yaml

from common.config.settings import PROJECT_ROOT

# load_scenario_idx를 10칸으로 나눠 0~7은 학습, 8은 검증, 9는 시험.
# 데이터가 늘어나도 같은 부하 시나리오는 늘 같은 쪽에 남아 champion과 후보를 같은 시험 데이터로 비교할 수 있음
SPLIT_BUCKETS = 10
VAL_BUCKET = 8
TEST_BUCKET = 9


def write_scenario_splits(config_path, data_dir):
    with open(PROJECT_ROOT / config_path) as f:
        config = yaml.safe_load(f)
    network = config["data"]["networks"][0]
    bus = pd.read_parquet(
        PROJECT_ROOT / data_dir / network / "raw" / "bus_data.parquet",
        columns=["scenario", "load_scenario_idx"],
    ).drop_duplicates("scenario")

    buckets = bus["load_scenario_idx"] % SPLIT_BUCKETS
    splits = {
        "train": bus["scenario"][buckets < VAL_BUCKET],
        "val": bus["scenario"][buckets == VAL_BUCKET],
        "test": bus["scenario"][buckets == TEST_BUCKET],
    }
    splits_dir = PROJECT_ROOT / config["data"]["split_from_existing_files"]
    splits_dir.mkdir(exist_ok=True)
    for name, scenarios in splits.items():
        torch.save(torch.tensor(sorted(scenarios)), splits_dir / f"{name}.pt")
