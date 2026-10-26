# [Airflow] 최적 조류(OPF) 예측 모델의 입력 분포 비교: 시나리오별 총 부하, 선로·발전기 정지 수, 모선별 유효 부하
# 점검 DAG(common/monitoring/drift.py)가 measure_recent_drift, 학습 DAG가 measure_training_drift를 부름
import pandas as pd
from sqlalchemy import create_engine, text

from common.monitoring.drift import compute_psi
from common.config.settings import DATABASE_URL, PROJECT_ROOT
from common.model_registry.tracking import fetch_run_tags
from sources.optimal_power_flow.training.data import fetch_network_name


def read_dataset_inputs(data_dir, network):
    # 학습용으로 내보낸 parquet (datakit 형식)
    raw_dir = PROJECT_ROOT / data_dir / network / "raw"
    bus = pd.read_parquet(raw_dir / "bus_data.parquet", columns=["scenario", "bus", "Pd", "Qd"])
    branch = pd.read_parquet(raw_dir / "branch_data.parquet", columns=["scenario", "br_status"])
    gen = pd.read_parquet(raw_dir / "gen_data.parquet", columns=["scenario", "in_service"])
    return bus, branch, gen


def read_realtime_inputs(source, first_scenario, last_scenario):
    # 실시간 테이블에서 first_scenario 이상 last_scenario 미만
    engine = create_engine(DATABASE_URL)
    params = {"first": first_scenario, "last": last_scenario}
    where = "WHERE scenario >= :first AND scenario < :last"
    bus = pd.read_sql(text(f'SELECT scenario, bus, "Pd", "Qd" FROM {source}.bus_realtime {where}'), engine, params=params)
    branch = pd.read_sql(text(f"SELECT scenario, br_status FROM {source}.branch_realtime {where}"), engine, params=params)
    gen = pd.read_sql(text(f"SELECT scenario, in_service FROM {source}.gen_realtime {where}"), engine, params=params)
    return bus, branch, gen


def fetch_realtime_scenario_count(source):
    engine = create_engine(DATABASE_URL)
    return int(pd.read_sql(f"SELECT COUNT(DISTINCT scenario) FROM {source}.bus_realtime", engine).iloc[0, 0])


def summarize_scenarios(bus, branch, gen):
    summary = bus.groupby("scenario")[["Pd", "Qd"]].sum().rename(columns={"Pd": "total_pd", "Qd": "total_qd"})
    summary["branch_outages"] = (branch["br_status"] == 0).groupby(branch["scenario"]).sum()
    summary["gen_outages"] = (gen["in_service"] == 0).groupby(gen["scenario"]).sum()
    return summary


def measure_data_drift(reference, current, psi_threshold):
    # reference, current: read_*_inputs의 (bus, branch, gen). 이름이 _psi로 끝나는 값과 out_of_range_ratio로 판정함
    reference_summary = summarize_scenarios(*reference)
    current_summary = summarize_scenarios(*current)
    measures = {
        f"{name}_psi": compute_psi(reference_summary[name], current_summary[name])
        for name in reference_summary.columns
    }
    measures["reference_total_pd_min"] = float(reference_summary["total_pd"].min())
    measures["reference_total_pd_max"] = float(reference_summary["total_pd"].max())

    reference_pd = reference[0].pivot(index="scenario", columns="bus", values="Pd")
    current_pd = current[0].pivot(index="scenario", columns="bus", values="Pd")
    bus_psi = [compute_psi(reference_pd[bus], current_pd[bus]) for bus in reference_pd.columns]
    measures["max_bus_pd_psi"] = max(bus_psi)
    measures["shifted_bus_count"] = int(sum(psi >= psi_threshold for psi in bus_psi))
    outside = (current_pd < reference_pd.min()) | (current_pd > reference_pd.max())
    measures["out_of_range_ratio"] = float(outside.to_numpy().mean())
    return measures


def read_training_inputs(spec, run_id):
    # champion 학습 run의 태그(describe_dataset)로 그 run이 학습한 입력을 다시 읽음. 실시간 데이터로 학습했으면 학습한 시나리오 수도 돌려줌
    tags = fetch_run_tags(run_id)
    data_dir = tags[f"{spec['data_source']}.data_dir"]
    if data_dir == spec["training"]["data_dir"]:
        trained_count = int(tags[f"{spec['data_source']}.{fetch_network_name(spec)}.scenario_count"])
        return read_realtime_inputs(spec["data_source"], 0, trained_count), trained_count
    return read_dataset_inputs(data_dir, fetch_network_name(spec)), 0


def measure_recent_drift(spec, champion_run_id):
    # 학습 뒤에 들어온 최근 drift.window개 시나리오를 champion 학습 입력과 비교. 아직 모자라면 None
    reference, trained_count = read_training_inputs(spec, champion_run_id)
    window = spec["drift"]["window"]
    total_count = fetch_realtime_scenario_count(spec["data_source"])
    first_scenario = max(trained_count, total_count - window)
    if total_count - first_scenario < window:
        return None
    current = read_realtime_inputs(spec["data_source"], first_scenario, total_count)
    return first_scenario, total_count - 1, measure_data_drift(reference, current, spec["drift"]["psi_threshold"])


def measure_training_drift(spec, champion_run_id, data_dir):
    # 새 학습 데이터(data_dir)를 champion 학습 입력과 비교
    reference, _ = read_training_inputs(spec, champion_run_id)
    current = read_dataset_inputs(data_dir, fetch_network_name(spec))
    return measure_data_drift(reference, current, spec["drift"]["psi_threshold"])
