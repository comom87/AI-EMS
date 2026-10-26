import os
import subprocess

from common.config.settings import PROJECT_ROOT
from common.config.source_specs import fetch_data_source_dir

# datakit 컨테이너 안에서 configs/는 data/<소스>/configs/(실행할 때 연결), data/는 호스트의 data/ (infra/docker-compose.yml)
COMPOSE_FILE = PROJECT_ROOT / "infra" / "docker-compose.yml"
DATASETS_DIR = PROJECT_ROOT / "data"

# 컨테이너는 root로 돌아가므로(Julia 패키지가 /root에 설치됨) 끝나면 만든 파일을 실행한 사람 소유로 돌려놓음
DATAKIT_SCRIPT = 'gridfm_datakit "$@"; status=$?; chown -R "$OWNER" /workspace/data; exit $status'


def run_datakit(source, *args):
    # 연결할 폴더가 없으면 Docker가 root 소유로 만들어 버리므로 먼저 만듦
    DATASETS_DIR.mkdir(exist_ok=True)
    subprocess.run(
        [
            "docker", "compose", "-f", str(COMPOSE_FILE), "run", "--rm",
            "-v", f"{fetch_data_source_dir(source) / 'configs'}:/workspace/configs",
            "-e", f"OWNER={os.getuid()}:{os.getgid()}",
            "datakit", "bash", "-c", DATAKIT_SCRIPT, "gridfm_datakit", *args,
        ],
        cwd=PROJECT_ROOT,
        check=True,
    )


def generate_dataset(source, config_path):
    # config_path는 컨테이너 기준 (예: configs/kpg193.yaml). 결과는 data/<설정의 data_dir>
    run_datakit(source, "generate", config_path)


def validate_dataset(source, data_dir):
    run_datakit(source, "validate", data_dir)


def compute_dataset_stats(source, data_dir):
    run_datakit(source, "stats", data_dir)
