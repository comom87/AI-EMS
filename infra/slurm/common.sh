#!/bin/bash
# Slurm 작업 스크립트(common/slurm/sbatch_script.py가 만듦)가 source해서 쓰는 함수 모음입니다.
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if [ -f "${PROJECT_ROOT}/.env" ]; then
    set -a
    . "${PROJECT_ROOT}/.env"
    set +a
fi
SIF_DIR="${AIEMS_SIF_DIR:-${PROJECT_ROOT}/infra/slurm/sif}"

# Docker 이미지 <image>:latest가 SIF를 만든 이미지와 다르면 SIF를 다시 만듦. run_container가 작업을 시작할 때 부름.
# 모델 개발자는 docker build만 하면 되고, 이미지가 그대로면 바로 지나감.
# Docker 이미지가 없거나 Docker·SIF 폴더를 쓸 수 없는 사용자의 작업이면 있는 SIF를 그대로 씀
update_sif_image() {
    local image="$1"
    local sif="${SIF_DIR}/${image}.sif"
    local image_id
    image_id=$(docker image inspect -f '{{.Id}}' "${image}:latest" 2>/dev/null) || return 0
    [ -w "$SIF_DIR" ] || return 0
    # 같은 이미지를 쓰는 작업이 동시에 시작해도 한 작업만 변환하고 나머지는 기다렸다가 새 SIF를 씀
    local lock_fd status=0
    exec {lock_fd}>"${sif}.lock"
    flock "$lock_fd"
    # <image>.sif.id: 이 SIF를 만든 Docker 이미지 ID. 다 만든 뒤에 바꿔서, 변환 중인 SIF를 다른 작업이 읽지 않게 함
    if [ "$(cat "${sif}.id" 2>/dev/null)" != "$image_id" ]; then
        apptainer build --force "${sif}.building" "docker-daemon://${image}:latest" \
            && mv "${sif}.building" "$sif" \
            && echo "$image_id" > "${sif}.id"
        status=$?
        rm -f "${sif}.building"
    fi
    exec {lock_fd}>&-
    return $status
}

run_container() {
    local image="$1"
    shift
    update_sif_image "$image" || return 1
    apptainer run --nv \
        --bind "${PROJECT_ROOT}:/workspace" \
        --pwd /workspace \
        --env MLFLOW_TRACKING_URI="${AIEMS_MLFLOW_TRACKING_URI}" \
        "${SIF_DIR}/${image}.sif" \
        "$@"
}
