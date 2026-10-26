# Slurm 작업 제출과 기다리기. 자원은 resources.py가 정하고, 기록은 job_records.py·job_history.py에 남김
import hashlib
import re
import subprocess
import time

from common.slurm.job_history import record_job_cpu_usage, record_memory_shortage
from common.slurm.job_records import record_job_submission
from common.slurm.resources import (
    QOS_MAX_WALL,
    calculate_gpu_memory_limit,
    estimate_memory_after_oom,
    estimate_resources,
    select_qos,
)
from common.config.settings import PROJECT_ROOT
from common.slurm.sbatch_script import build_sbatch_script

LOG_DIR = PROJECT_ROOT / "infra" / "slurm" / "logs"
GPU_OOM_PATTERN = re.compile(r"CUDA (error: )?out of memory")


def fetch_job_key(image, command):
    return hashlib.sha256(f"{image} {command}".encode()).hexdigest()


def wait_for_job(job_id):
    while subprocess.run(["squeue", "-j", job_id, "--noheader"], capture_output=True, text=True).stdout.strip():
        time.sleep(5)


def fetch_job_state(job_id):
    result = subprocess.run(
        ["sacct", "-j", job_id, "-X", "--format=State", "--noheader", "--parsable2"],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def fetch_job_log(job_id):
    return (LOG_DIR / f"model-train-{job_id}.out").read_text()


def submit_training_job(
    image,
    command,
    job_key=None,
    job_label=None,
    gres=None,
    cpus_per_task=None,
    mem=None,
    gpu_mem=None,
    qos=None,
    time=None,
    wait=False,
):
    job_key = job_key or fetch_job_key(image, command)

    if gres is None or cpus_per_task is None or mem is None or gpu_mem is None:
        resources = estimate_resources(job_key)
        gres = gres or resources["gres"]
        cpus_per_task = cpus_per_task or resources["cpus_per_task"]
        mem = mem or resources["mem"]
        gpu_mem = gpu_mem or resources["gpu_mem"]

    if qos is None:
        qos = select_qos(int(gres.split(":")[1]))

    if time is None:
        time = QOS_MAX_WALL[qos]

    gpu_mem_limit_gb = calculate_gpu_memory_limit(gres, gpu_mem)
    script = build_sbatch_script(image, command, job_key, gres, cpus_per_task, mem, gpu_mem_limit_gb, qos, time)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(["sbatch"], input=script, check=True, capture_output=True, text=True)
    job_id = result.stdout.strip().split()[-1]
    record_job_submission(job_id, job_key, job_label or job_key[:12], image, command, qos, cpus_per_task, mem, gres, gpu_mem_limit_gb)

    if wait:
        wait_for_job(job_id)
        state = fetch_job_state(job_id)
        if state != "COMPLETED":
            if state == "OUT_OF_MEMORY":
                record_memory_shortage(job_key, "cpu_mem_gb", estimate_memory_after_oom(int(mem.rstrip("G"))))
            if GPU_OOM_PATTERN.search(fetch_job_log(job_id)):
                record_memory_shortage(job_key, "gpu_mem_gb", estimate_memory_after_oom(gpu_mem_limit_gb))
            raise RuntimeError(f"Slurm job {job_id} finished with state {state}")
        record_job_cpu_usage(job_key, job_id)

    return job_id
