-- Slurm 학습 작업 기록 (common 스키마, common/slurm/job_history.py와 작업 스크립트가 씀). 모든 사용자와 모델이 함께 씀
-- 여러 번 실행해도 안전합니다 (이미 있는 스키마·테이블·인덱스는 건너뜀).
-- 열 이름 규칙: alloc_* = Slurm에서 받은 몫(제출할 때), used_* = 실제로 쓴 양(끝난 뒤). cpu_* / gpu_*로 자원을 구분

CREATE SCHEMA IF NOT EXISTS common;

-- 제출한 작업 1건: 누가 무엇을 제출했고, 무엇을 받았고, 실제로 얼마나 썼는지
CREATE TABLE IF NOT EXISTS common.slurm_jobs (
    job_id bigint PRIMARY KEY,
    job_key text NOT NULL,                  -- 같은 종류의 작업을 묶는 지문 (이미지 + 명령, run 이름 제외)
    job_label text NOT NULL,                -- 사람이 알아보는 이름 (예: 학습 run 이름)
    submitted_by text NOT NULL,             -- 제출한 리눅스 계정
    image text NOT NULL,
    command text NOT NULL,
    qos text NOT NULL,                      -- small, urgent, default, large
    submitted_at timestamptz NOT NULL,
    finished_at timestamptz,                -- 실행 중이면 비어 있음
    exit_code integer,
    state text,                             -- COMPLETED, FAILED, OUT_OF_MEMORY, TIMEOUT 등
    alloc_cpus integer NOT NULL,            -- 받은 CPU 코어 수
    alloc_cpu_mem_gb double precision NOT NULL,  -- 받은 CPU 메모리 (--mem, GB)
    alloc_mps_percent integer NOT NULL,     -- 받은 GPU SM 몫 (MPS %, GPU 1장 = 100)
    alloc_gpu_mem_gb double precision NOT NULL,  -- 걸어 둔 GPU 메모리 한도 (GB)
    used_cpu_cores double precision,        -- CPU 코어 95번째 백분위 (작업 cgroup)
    used_cpu_mem_gb double precision,       -- 최대 CPU 메모리 (cgroup memory.peak, GB). Slurm이 --mem 한도를 적용하는 양
    used_sm_percent double precision,       -- GPU SM 사용률 95번째 백분위 (%, GPU 1장 전체 기준)
    used_gpu_mem_gb double precision        -- 이 작업 프로세스의 최대 GPU 메모리 (GB)
);
CREATE INDEX IF NOT EXISTS slurm_jobs_key ON common.slurm_jobs (job_key);
CREATE INDEX IF NOT EXISTS slurm_jobs_unfinished ON common.slurm_jobs (job_id) WHERE finished_at IS NULL;

-- 작업 중 5초마다 잰 사용량. 작업 스크립트가 직접 씀
CREATE TABLE IF NOT EXISTS common.slurm_job_usage (
    job_id bigint NOT NULL,
    sampled_at timestamptz NOT NULL,
    sm_active double precision,             -- SM 사용률 (0~1, GPU 1장 전체 기준)
    gpu_mem_mb double precision,            -- 이 작업 프로세스의 GPU 메모리
    cpu_cores double precision,             -- 이 작업이 쓴 CPU 코어 수 (작업 cgroup)
    cpu_mem_mb double precision             -- 이 작업의 CPU 메모리 (작업 cgroup)
);
CREATE INDEX IF NOT EXISTS slurm_job_usage_job_time ON common.slurm_job_usage (job_id, sampled_at);

-- 같은 종류의 작업(job_key)별 누적 기록. 다음 제출 때 이 값 × 1.3을 요청함. 모든 값은 지금까지 중 가장 큰 값으로 갱신
CREATE TABLE IF NOT EXISTS common.slurm_job_history (
    job_key text PRIMARY KEY,
    sm_percent double precision,            -- MPS 100%로 돌린 작업들의 used_sm_percent 중 최댓값 (%)
    gpu_mem_gb double precision,            -- used_gpu_mem_gb 중 최댓값 (GB)
    gpu_count integer,                      -- GPU 사용량을 기록한 횟수
    cpu_cores double precision,             -- used_cpu_cores 중 최댓값. 할당에 거의 닿은(꽉 찬) 작업은 alloc_cpus
    cpu_mem_gb double precision,            -- used_cpu_mem_gb 중 최댓값 (GB)
    cpu_count integer                       -- CPU 사용량을 기록한 횟수
);
