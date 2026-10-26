# Slurm 작업 한 건의 기록 (common.slurm_jobs): 제출, 받은 몫, 종료 상태, 메모리 최고점. 테이블 정의는 db/slurm_jobs.sql
import getpass
import os
import subprocess
from datetime import datetime

from sqlalchemy import create_engine, text

from common.config.settings import DATABASE_URL


def connect_history_db():
    return create_engine(DATABASE_URL)


def fetch_submitter_name():
    # 컨테이너(Airflow 등)는 uid만 있고 사용자 정보가 없을 수 있어서, 그때는 uid 번호를 남김
    try:
        return getpass.getuser()
    except KeyError:
        return f"uid:{os.getuid()}"


def record_job_submission(job_id, job_key, job_label, image, command, qos, cpus_per_task, mem, gres, gpu_mem_limit_gb):
    # 제출한 작업과 Slurm에서 받은 몫. 실제 사용량(used_*)은 작업이 끝난 뒤 채움
    with connect_history_db().begin() as connection:
        connection.execute(
            text(
                "INSERT INTO common.slurm_jobs (job_id, job_key, job_label, submitted_by, image, command, qos, submitted_at, "
                "alloc_cpus, alloc_cpu_mem_gb, alloc_mps_percent, alloc_gpu_mem_gb) "
                "VALUES (:job_id, :job_key, :job_label, :submitted_by, :image, :command, :qos, now(), "
                ":alloc_cpus, :alloc_cpu_mem_gb, :alloc_mps_percent, :alloc_gpu_mem_gb)"
            ),
            {
                "job_id": job_id,
                "job_key": job_key,
                "job_label": job_label,
                "submitted_by": fetch_submitter_name(),
                "image": image,
                "command": command,
                "qos": qos,
                "alloc_cpus": cpus_per_task,
                "alloc_cpu_mem_gb": float(mem.rstrip("G")),
                "alloc_mps_percent": int(gres.split(":")[1]),
                "alloc_gpu_mem_gb": gpu_mem_limit_gb,
            },
        )


def record_finished_jobs():
    with connect_history_db().begin() as connection:
        job_ids = connection.execute(
            text("SELECT job_id FROM common.slurm_jobs WHERE finished_at IS NULL")
        ).scalars().all()
        for job_id in job_ids:
            result = subprocess.run(
                ["sacct", "-j", str(job_id), "-X", "--format=End,ExitCode,State", "--noheader", "--parsable2"],
                capture_output=True,
                text=True,
                check=True,
            )
            end, exit_code, state = result.stdout.strip().split("|")
            if end == "Unknown":
                continue
            code, signal = (int(value) for value in exit_code.split(":"))
            connection.execute(
                text(
                    "UPDATE common.slurm_jobs SET finished_at = :finished_at, exit_code = :exit_code, state = :state "
                    "WHERE job_id = :job_id"
                ),
                {
                    "finished_at": datetime.fromisoformat(end).astimezone(),
                    "exit_code": 128 + signal if signal else code,
                    "state": state.split()[0],
                    "job_id": job_id,
                },
            )


def fetch_job(job_id):
    with connect_history_db().connect() as connection:
        row = connection.execute(
            text("SELECT alloc_cpus, used_cpu_mem_gb FROM common.slurm_jobs WHERE job_id = :job_id"),
            {"job_id": job_id},
        ).mappings().first()
    return dict(row) if row is not None else None


def record_job_end(job_id, exit_code):
    with connect_history_db().begin() as connection:
        connection.execute(
            text(
                "UPDATE common.slurm_jobs SET finished_at = now(), exit_code = :exit_code, "
                "state = CASE WHEN :exit_code = 0 THEN 'COMPLETED' ELSE 'FAILED' END WHERE job_id = :job_id"
            ),
            {"exit_code": exit_code, "job_id": job_id},
        )


def record_memory_peak(job_id, memory_peak_bytes):
    # 작업 cgroup의 memory.peak (Slurm이 --mem 한도를 적용하는 양의 최고점). 작업 스크립트가 끝날 때 부름
    if not memory_peak_bytes:
        return
    with connect_history_db().begin() as connection:
        connection.execute(
            text("UPDATE common.slurm_jobs SET used_cpu_mem_gb = :peak_gb WHERE job_id = :job_id"),
            {"peak_gb": int(memory_peak_bytes) / 1024**3, "job_id": job_id},
        )
