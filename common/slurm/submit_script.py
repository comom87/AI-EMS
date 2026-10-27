# 파이프라인 없이 학습 파일 하나를 Slurm 작업으로 내는 제출 도구. 모델 개발자가 서버 터미널에서 직접 실행함
# 사용: .venv/bin/python3 -m common.slurm.submit_script sources/<기능>/<학습 파일>.py [학습 파일에 넘길 인자...]
# 파일 경로로 기능 폴더(= 모델)를 알아내고, 그 폴더의 training/model_train.py의 TRAINING_IMAGE(없으면 공용 graphkit 이미지)로 명령·자원을 채운 뒤 제출만 하고 끝남
# 진행은 squeue, 출력은 infra/slurm/logs/model-train-<작업 번호>.out, 결과는 MLflow 실험 <모델 이름>에서 봄
import shlex
import sys
from pathlib import Path

from common.config.model_specs import FUNCTIONS_DIR, fetch_model_dir, import_function_module, load_model_spec
from common.config.settings import PROJECT_ROOT
from common.slurm.scheduling import submit_training_job

# 기능 폴더에 training/model_train.py가 없을 때 쓰는 공용 학습 이미지
DEFAULT_TRAINING_IMAGE = "aiems-graphkit"


def fetch_script_path(script):
    # 저장소 기준 경로로 바꿈. 컨테이너 안에서는 저장소 루트가 /workspace라서 이 경로 그대로 실행됨
    path = Path(script).resolve()
    if not path.is_file() or path.suffix != ".py":
        raise ValueError(f"{script} is not a Python file")
    if not path.is_relative_to(FUNCTIONS_DIR):
        raise ValueError(f"{script} must be inside sources/<function>/")
    return path.relative_to(PROJECT_ROOT)


def fetch_training_image(name):
    # 학습 파이프라인과 같은 이미지를 쓰도록 기능 폴더 training/model_train.py의 TRAINING_IMAGE를 읽음
    if not (fetch_model_dir(name) / "training" / "model_train.py").exists():
        return DEFAULT_TRAINING_IMAGE
    return import_function_module(name, "training.model_train").TRAINING_IMAGE


def build_script_job(script, script_args):
    # 학습 파일 → Slurm 작업 내용
    script_path = fetch_script_path(script)
    spec = load_model_spec(script_path.parts[1])
    command = shlex.join(["python3", str(script_path), *script_args])
    if '"' in command:
        raise ValueError("Script arguments cannot contain double quotes")
    return {
        "image": fetch_training_image(spec["name"]),
        # 학습 이미지는 bash -c 인자를 한 번 더 셸로 풀기 때문에 안쪽에는 작은따옴표만 씀
        "command": f'bash -c "{command}"',
        # 같은 파일·인자로 다시 내면 지난번 사용량으로 자원을 정함
        "job_label": f"{spec['name']}_manual",
    }


def submit_script_job(script, script_args):
    submit_training_job(**build_script_job(script, script_args))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python3 -m common.slurm.submit_script sources/<function>/<script>.py [args...]")
    submit_script_job(sys.argv[1], sys.argv[2:])
