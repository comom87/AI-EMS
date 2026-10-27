# 플랫폼 파이프라인의 operator: 종료 기록이 빠진 Slurm 작업 채우기
# 모든 단계는 (spec, context, **args)를 받음. 자세한 약속은 dags/operators/__init__.py
from common.slurm.job_records import record_finished_jobs


def record_job_endings(spec, context):
    # 기다리지 않고 제출했거나 비정상 종료해서 종료 기록이 비어 있는 Slurm 작업의 종료 상태를 sacct로 채움 (플랫폼용)
    record_finished_jobs()
