# 같은 작업(job_key)의 누적 자원 기록 (common.slurm_job_history): 다음 요청량의 기준. 테이블 정의는 db/slurm_jobs.sql
from sqlalchemy import text

from common.slurm.job_records import connect_history_db, fetch_job
from common.slurm.job_usage import fetch_usage_summary

HISTORY_COLUMNS = ["sm_percent", "gpu_mem_gb", "gpu_count", "cpu_cores", "cpu_mem_gb", "cpu_count"]
# CPU 코어 95번째 백분위가 할당 코어의 이 비율 이상이면 '꽉 찬 작업'. 실제 필요량은 알 수 없으므로 할당 코어 수를 기록해 다음에 더 줌
CPU_SATURATION_RATIO = 0.9


def fetch_job_history(job_key):
    with connect_history_db().connect() as connection:
        row = connection.execute(
            text(f"SELECT {', '.join(HISTORY_COLUMNS)} FROM common.slurm_job_history WHERE job_key = :job_key"),
            {"job_key": job_key},
        ).mappings().first()
    return dict(row) if row is not None else None


def record_job_usage(job_key, job_id, sm_limit_percent):
    # 작업 스크립트가 끝날 때 부름 (사용량을 DB에 다 올린 뒤)
    usage = fetch_usage_summary(job_id)
    if usage["sm_active"] is None or usage["gpu_mem_mb"] is None:
        return

    sm_percent = usage["sm_active"] * 100
    gpu_mem_gb = usage["gpu_mem_mb"] / 1024
    previous = fetch_job_history(job_key) or {}
    # MPS로 SM을 제한한 작업의 값은 제한 이하로만 나와서 실제 필요량을 알 수 없음. 제한 없이(100%) 돌린 작업의 값만 기록함
    history_sm_percent = previous.get("sm_percent")
    if sm_limit_percent == 100:
        history_sm_percent = select_max_value(history_sm_percent, sm_percent)

    with connect_history_db().begin() as connection:
        connection.execute(
            text(
                "INSERT INTO common.slurm_job_history (job_key, sm_percent, gpu_mem_gb, gpu_count) "
                "VALUES (:job_key, :sm_percent, :gpu_mem_gb, :gpu_count) "
                "ON CONFLICT (job_key) DO UPDATE SET sm_percent = excluded.sm_percent, "
                "gpu_mem_gb = excluded.gpu_mem_gb, gpu_count = excluded.gpu_count"
            ),
            {
                "job_key": job_key,
                "sm_percent": history_sm_percent,
                "gpu_mem_gb": select_max_value(previous.get("gpu_mem_gb"), gpu_mem_gb),
                "gpu_count": (previous.get("gpu_count") or 0) + 1,
            },
        )
        connection.execute(
            text("UPDATE common.slurm_jobs SET used_sm_percent = :sm_percent, used_gpu_mem_gb = :gpu_mem_gb WHERE job_id = :job_id"),
            {"sm_percent": sm_percent, "gpu_mem_gb": gpu_mem_gb, "job_id": job_id},
        )


def select_max_value(*values):
    # 비어 있는 값(None)은 빼고 가장 큰 값. 모두 비어 있으면 None
    present = [value for value in values if value is not None]
    return max(present) if present else None


def measure_cpu_cores(job_id, allocated_cores):
    # 기록에 반영할 CPU 코어 수. 할당 코어에 거의 닿은 작업은 실제 필요량이 더 클 수 있으므로 할당 코어 수를 돌려줌
    cpu_cores = fetch_usage_summary(job_id)["cpu_cores"]
    if cpu_cores is None:
        return None, None
    if cpu_cores >= allocated_cores * CPU_SATURATION_RATIO:
        return cpu_cores, allocated_cores
    return cpu_cores, cpu_cores


def record_job_cpu_usage(job_key, job_id):
    # 작업 cgroup에서 잰 값만 씀. 코어는 5초 간격 사용량의 95번째 백분위, 메모리는 memory.peak
    # 누적 기록은 SM·GPU 메모리와 같이 지금까지 중 가장 큰 값을 남김
    job = fetch_job(job_id)
    cpu_cores_p95, cpu_cores = measure_cpu_cores(job_id, job["alloc_cpus"])
    previous = fetch_job_history(job_key) or {}

    with connect_history_db().begin() as connection:
        connection.execute(
            text(
                "INSERT INTO common.slurm_job_history (job_key, cpu_cores, cpu_mem_gb, cpu_count) "
                "VALUES (:job_key, :cpu_cores, :cpu_mem_gb, :cpu_count) "
                "ON CONFLICT (job_key) DO UPDATE SET cpu_cores = excluded.cpu_cores, "
                "cpu_mem_gb = excluded.cpu_mem_gb, cpu_count = excluded.cpu_count"
            ),
            {
                "job_key": job_key,
                "cpu_cores": select_max_value(previous.get("cpu_cores"), cpu_cores),
                "cpu_mem_gb": select_max_value(previous.get("cpu_mem_gb"), job["used_cpu_mem_gb"]),
                "cpu_count": (previous.get("cpu_count") or 0) + 1,
            },
        )
        connection.execute(
            text("UPDATE common.slurm_jobs SET used_cpu_cores = :cpu_cores WHERE job_id = :job_id"),
            {"cpu_cores": cpu_cores_p95, "job_id": job_id},
        )


def record_memory_shortage(job_key, column, memory_gb):
    # 메모리 부족(OOM)으로 실패한 작업의 다음 요청량을 바로 늘려 둠. column은 cpu_mem_gb 또는 gpu_mem_gb
    with connect_history_db().begin() as connection:
        connection.execute(
            text(
                f"INSERT INTO common.slurm_job_history (job_key, {column}) VALUES (:job_key, :memory_gb) "
                f"ON CONFLICT (job_key) DO UPDATE SET {column} = excluded.{column}"
            ),
            {"job_key": job_key, "memory_gb": memory_gb},
        )
