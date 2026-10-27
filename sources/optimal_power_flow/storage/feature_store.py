# [Feast] 최적 조류(OPF) 예측 기능이 서빙 때 쓰는 피처 묶음 (Feast FeatureService)
# 표마다 행을 가리키는 열이 달라서(모선 bus, 발전기·선로 idx) 표마다 묶음을 하나씩 둠
# 적용: common/storage/feature_store.py의 apply_feature_definitions()
from feast import FeatureService

from data.kpg193.storage.feature_store import branch_view, bus_view, gen_view

BUS_INPUTS = ["Pd", "Qd", "Qg", "Vm", "Va", "PQ", "PV", "REF", "min_vm_pu", "max_vm_pu", "GS", "BS", "vn_kv"]
GEN_INPUTS = [
    "bus", "p_mw", "min_p_mw", "max_p_mw", "min_q_mvar", "max_q_mvar",
    "cp0_eur", "cp1_eur_per_mw", "cp2_eur_per_mw2", "in_service",
]
BRANCH_INPUTS = [
    "from_bus", "to_bus", "pf", "qf", "pt", "qt", "Yff_r", "Yff_i", "Yft_r", "Yft_i",
    "Ytf_r", "Ytf_i", "Ytt_r", "Ytt_i", "tap", "ang_min", "ang_max", "rate_a", "br_status",
]


def fetch_input_service_name(source, table):
    # 예: optimal_power_flow_kpg193_bus
    return f"optimal_power_flow_{source}_{table}"


kpg193_bus_inputs = FeatureService(name=fetch_input_service_name("kpg193", "bus"), features=[bus_view[BUS_INPUTS]])
kpg193_gen_inputs = FeatureService(name=fetch_input_service_name("kpg193", "gen"), features=[gen_view[GEN_INPUTS]])
kpg193_branch_inputs = FeatureService(name=fetch_input_service_name("kpg193", "branch"), features=[branch_view[BRANCH_INPUTS]])
