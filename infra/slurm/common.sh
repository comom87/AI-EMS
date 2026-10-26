#!/bin/bash
# Slurm 작업 스크립트(common/slurm/sbatch_script.py가 만듦)가 source해서 쓰는 함수 모음입니다.
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if [ -f "${PROJECT_ROOT}/.env" ]; then
    set -a
    . "${PROJECT_ROOT}/.env"
    set +a
fi
SIF_DIR="${AIEMS_SIF_DIR:-${PROJECT_ROOT}/infra/slurm/sif}"

run_container() {
    local image="$1"
    shift
    apptainer run --nv \
        --bind "${PROJECT_ROOT}:/workspace" \
        --pwd /workspace \
        --env MLFLOW_TRACKING_URI="${AIEMS_MLFLOW_TRACKING_URI}" \
        "${SIF_DIR}/${image}.sif" \
        "$@"
}
