import math

from common.slurm.job_history import fetch_job_history

MEM_PER_CORE_GB = 4
GPU_TOTAL_MEM_GB = 24
OOM_MEMORY_GROWTH = 2
USAGE_MARGIN = 1.3
DEFAULT_CPUS_PER_TASK = 8
# 노드 하나의 CPU 수 (infra/slurm/etc/slurm.conf의 CPUs). 넘게 요청하면 sbatch가 제출을 거부함
MAX_CPUS_PER_TASK = 16
DEFAULT_GPU_MEM_GB = 12
PROFILING_MPS_PERCENT = 100
MIN_MPS_PERCENT = 10

QOS_MAX_WALL = {
    "small": "02:00:00",
    "urgent": "04:00:00",
    "default": "12:00:00",
    "large": "2-00:00:00",
}


def estimate_mps_percent(sm_percent):
    if sm_percent is None:
        return PROFILING_MPS_PERCENT
    return min(PROFILING_MPS_PERCENT, max(MIN_MPS_PERCENT, math.ceil(sm_percent * USAGE_MARGIN)))


def estimate_resources(job_key):
    history = fetch_job_history(job_key)
    if history is None:
        return {
            "gres": f"mps:{PROFILING_MPS_PERCENT}",
            "cpus_per_task": DEFAULT_CPUS_PER_TASK,
            "mem": f"{DEFAULT_CPUS_PER_TASK * MEM_PER_CORE_GB}G",
            "gpu_mem": f"{DEFAULT_GPU_MEM_GB}G",
        }

    # SM·메모리와 같이 여유분을 더함. 꽉 찬 작업은 할당 코어 수가 기록돼 있어서 다음에는 그보다 많이 받음 (노드 CPU 수까지)
    cpus_per_task = (
        min(MAX_CPUS_PER_TASK, max(1, math.ceil(history["cpu_cores"] * USAGE_MARGIN)))
        if history["cpu_cores"] is not None
        else DEFAULT_CPUS_PER_TASK
    )
    mem = f"{math.ceil(history['cpu_mem_gb'] * USAGE_MARGIN)}G" if history["cpu_mem_gb"] is not None else f"{cpus_per_task * MEM_PER_CORE_GB}G"
    gpu_mem = f"{math.ceil(history['gpu_mem_gb'] * USAGE_MARGIN)}G" if history["gpu_mem_gb"] is not None else f"{DEFAULT_GPU_MEM_GB}G"
    return {
        "gres": f"mps:{estimate_mps_percent(history['sm_percent'])}",
        "cpus_per_task": cpus_per_task,
        "mem": mem,
        "gpu_mem": gpu_mem,
    }


def select_qos(mps_percent):
    if mps_percent < 30:
        return "small"
    if mps_percent >= 70:
        return "large"
    return "default"


def calculate_gpu_memory_limit(gres, gpu_mem):
    return max(GPU_TOTAL_MEM_GB * int(gres.split(":")[1]) / 100, int(gpu_mem.rstrip("G")))


def estimate_memory_after_oom(allocated_gb):
    return allocated_gb * OOM_MEMORY_GROWTH / USAGE_MARGIN
