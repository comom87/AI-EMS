# 작업 중 5초마다 잰 사용량 (common.slurm_job_usage): 작업 스크립트가 남긴 CSV를 올리고 요약함. 테이블 정의는 db/slurm_jobs.sql
from pathlib import Path

from sqlalchemy import text

from common.slurm.job_records import connect_history_db

# SM과 CPU 코어는 5초 간격 사용량의 95번째 백분위로 봄 (짧게 튀는 값은 빼고 거의 최대치)
USAGE_PERCENTILE = 0.95


def read_usage_samples(usage_log_path):
    path = Path(usage_log_path)
    if not path.exists():
        return []
    samples = []
    for line in path.read_text().strip().splitlines():
        values = [value.strip() for value in line.split(",")]
        sampled_at, sm_active, gpu_mem_mb, cpu_cores, cpu_mem_mb = [float(value) if value else None for value in values]
        samples.append({
            "sampled_at": sampled_at,
            "sm_active": sm_active,
            "gpu_mem_mb": gpu_mem_mb,
            "cpu_cores": cpu_cores,
            "cpu_mem_mb": cpu_mem_mb,
        })
    return samples


def record_usage_samples(job_id, usage_log_path):
    samples = read_usage_samples(usage_log_path)
    with connect_history_db().begin() as connection:
        last_sampled_at = connection.execute(
            text("SELECT extract(epoch FROM max(sampled_at)) FROM common.slurm_job_usage WHERE job_id = :job_id"),
            {"job_id": job_id},
        ).scalar()
        new_samples = [sample for sample in samples if last_sampled_at is None or sample["sampled_at"] > last_sampled_at]
        if new_samples:
            connection.execute(
                text(
                    "INSERT INTO common.slurm_job_usage "
                    "VALUES (:job_id, to_timestamp(:sampled_at), :sm_active, :gpu_mem_mb, :cpu_cores, :cpu_mem_mb)"
                ),
                [{"job_id": job_id, **sample} for sample in new_samples],
            )


def fetch_usage_summary(job_id):
    # 작업 중 5초마다 잰 사용량(slurm_job_usage)의 요약. 값이 없는 칸은 빼고 계산함
    with connect_history_db().connect() as connection:
        row = connection.execute(
            text(
                "SELECT percentile_cont(:percentile) WITHIN GROUP (ORDER BY sm_active) AS sm_active, "
                "percentile_cont(:percentile) WITHIN GROUP (ORDER BY cpu_cores) AS cpu_cores, "
                "max(gpu_mem_mb) AS gpu_mem_mb "
                "FROM common.slurm_job_usage WHERE job_id = :job_id"
            ),
            {"percentile": USAGE_PERCENTILE, "job_id": job_id},
        ).mappings().first()
    return dict(row)
