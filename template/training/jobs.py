# [Airflow] 학습·평가 Slurm 작업 내용 만들기: 학습 DAG가 부르고, 돌려준 dict로 common/slurm/scheduling.py의 submit_training_job을 부름
# Airflow 안에서 import되므로 torch를 import하지 않음
# 돌려줄 dict: image(spec["training"]["image"], infra/slurm/sif/<image>.sif), command(컨테이너 안에서 저장소 루트 기준으로 실행할 명령),
#   job_key(자원 기록을 이어 쓸 키, run 이름은 빼고 만듦), cpus_per_task(선택). GPU·메모리는 지난 기록으로 플랫폼이 정함
from common.slurm.scheduling import fetch_job_key


def build_train_job(spec, config_path, data_dir, exp_name, run_name):
    # 컨테이너 안 학습 코드가 MLflow 실험 exp_name(모델 이름)에 run 이름 run_name으로 run을 만들고 모델 파일을 올려야 함
    image = spec["training"]["image"]
    command = (
        "python3 -c 'import sys; from sources.your_function.training.train import fit_model; "
        f"fit_model(*sys.argv[1:])' {config_path} {data_dir} {exp_name} {run_name}"
    )
    return {"image": image, "command": command, "job_key": fetch_job_key(image, f"fit_model {config_path} {data_dir} {exp_name}")}


def build_evaluate_job(spec, config_path, data_dir, exp_name, run_name, model_run_id, work_dir):
    # 모델 run(model_run_id: 후보 또는 champion)을 같은 시험 데이터로 평가함. run_name은 <학습 run>_eval 또는 <학습 run>_champion_eval
    # 컨테이너 안 평가 코드가 run 이름 run_name으로 run을 만들고 model.yaml evaluation.metric 지표를 기록해야 함 (승격 판단에 씀)
    # work_dir: 모델 파일을 내려받아 둘 수 있는 저장소 안 폴더 (학습 DAG가 끝나면 지움)
    image = spec["training"]["image"]
    command = (
        "python3 -c 'import sys; from sources.your_function.training.train import evaluate_model; "
        f"evaluate_model(*sys.argv[1:])' {config_path} {data_dir} {exp_name} {run_name} {model_run_id}"
    )
    return {"image": image, "command": command, "job_key": fetch_job_key(image, f"evaluate_model {config_path} {data_dir} {exp_name}")}
