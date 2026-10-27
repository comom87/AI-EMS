# [Airflow] 학습·평가 Slurm 작업 내용 만들기: 컨테이너 안에서 나누기 → graphkit → 결과 올리기를 차례로 실행함
# Airflow 안에서 import되므로 torch를 import하지 않음
from common.config.settings import PROJECT_ROOT
from common.model_registry.tracking import download_run_artifacts
from common.slurm.scheduling import fetch_job_key
from sources.optimal_power_flow.training.dataset_preparation import read_config

# 학습·평가를 실행할 Docker 이미지 (태그 latest). Slurm 작업이 시작할 때 SIF로 바꿔 씀. 제출 도구(common/slurm/submit_script.py)도 이 값을 씀
TRAINING_IMAGE = "aiems-graphkit"


def build_split_command(config_path, data_dir):
    return (
        "python3 -c 'import sys; from sources.optimal_power_flow.training.split_scenarios import write_scenario_splits; "
        f"write_scenario_splits(*sys.argv[1:])' {config_path} {data_dir}"
    )


def build_publish_command(exp_name, run_name):
    return (
        "python3 -c 'import sys; from sources.optimal_power_flow.training.publish_results import publish_run_results; "
        f"publish_run_results(*sys.argv[1:])' {exp_name} {run_name}"
    )


def build_graphkit_job(spec, config_path, data_dir, exp_name, run_name, graphkit_command, job_key_command):
    # 학습 이미지는 bash -c 인자를 한 번 더 셸로 풀기 때문에 안쪽에는 작은따옴표만 씀
    config = read_config(config_path)
    commands = [graphkit_command, build_publish_command(exp_name, run_name)]
    if "split_from_existing_files" in config["data"]:
        commands.insert(0, build_split_command(config_path, data_dir))
    return {
        "image": TRAINING_IMAGE,
        "command": f'bash -c "{" && ".join(commands)}"',
        "job_key": fetch_job_key(TRAINING_IMAGE, job_key_command),
        "cpus_per_task": config["data"]["workers"],
    }


def build_train_job(spec, config_path, data_dir, exp_name, run_name):
    train_command = (
        f"gridfm_graphkit train "
        f"--config {config_path} "
        f"--data_path {data_dir} "
        f"--log_dir mlruns "
        f"--exp_name {exp_name} "
        f"--run_name {run_name}"
    )
    # 작업 기록의 job_key에는 run 이름을 넣지 않아, 같은 학습은 매번 같은 자원 이력을 씀
    job_key_command = f"gridfm_graphkit train --config {config_path} --data_path {data_dir} --exp_name {exp_name}"
    return build_graphkit_job(spec, config_path, data_dir, exp_name, run_name, train_command, job_key_command)


def download_run_model(run_id, download_dir):
    # 평가에 넘길 모델 가중치와 정규화 통계 파일 경로 (저장소 기준 상대 경로, 컨테이너 안에서도 같음)
    download_dir = download_run_artifacts(run_id, download_dir)
    paths = [str(p.relative_to(download_dir)) for p in download_dir.rglob("*") if p.is_file()]
    model_path = next(p for p in paths if "model" in p and p.endswith(".pt"))
    stats_path = next(p for p in paths if "normalizer_stats" in p)
    download_relative_dir = download_dir.relative_to(PROJECT_ROOT)
    return str(download_relative_dir / model_path), str(download_relative_dir / stats_path)


def build_evaluate_job(spec, config_path, data_dir, exp_name, run_name, model_run_id, temp_dir):
    model_path, normalizer_stats = download_run_model(model_run_id, temp_dir)
    evaluate_command = (
        f"gridfm_graphkit evaluate "
        f"--config {config_path} "
        f"--data_path {data_dir} "
        f"--log_dir mlruns "
        f"--exp_name {exp_name} "
        f"--run_name {run_name} "
        f"--model_path {model_path} "
        f"--normalizer_stats {normalizer_stats} "
        f"--compute_dc_ac_metrics"
    )
    job_key_command = f"gridfm_graphkit evaluate --config {config_path} --data_path {data_dir} --exp_name {exp_name}"
    return build_graphkit_job(spec, config_path, data_dir, exp_name, run_name, evaluate_command, job_key_command)
