# Slurm에 넘길 작업 스크립트: 컨테이너 실행, 5초마다 사용량 재기, 끝난 뒤 사용량·종료 기록
from common.config.settings import PROJECT_ROOT

SBATCH_TEMPLATE = """#!/bin/bash -l
#SBATCH --job-name=model-train
#SBATCH --partition=gpu
#SBATCH --gres={gres}
#SBATCH --cpus-per-task={cpus_per_task}
#SBATCH --mem={mem}
#SBATCH --export=ALL
#SBATCH --qos={qos}
#SBATCH --time={time}
#SBATCH --output={project_root}/infra/slurm/logs/%x-%j.out

source {project_root}/infra/slurm/common.sh
cd {project_root}

usage_log={project_root}/infra/slurm/logs/${{SLURM_JOB_ID}}-usage.csv
record_samples() {{
    {project_root}/.venv/bin/python3 -c "from common.slurm.job_usage import record_usage_samples; record_usage_samples('$SLURM_JOB_ID', '$usage_log')"
}}
sample_count=0
job_cgroup=/sys/fs/cgroup$(sed -E 's#^0::(.*/job_[0-9]+)/.*#\\1#' /proc/self/cgroup)
previous_cpu_usec=$(awk '/^usage_usec/ {{print $2}}' "$job_cgroup/cpu.stat")
previous_ns=$(date +%s%N)
while true; do
    sleep 5
    sm_active=$(env -u CUDA_VISIBLE_DEVICES dcgmi dmon -i "$CUDA_VISIBLE_DEVICES" -e 1002 -c 1 | awk '/^GPU/ {{print $3}}')
    job_pids=$(scontrol listpids "$SLURM_JOB_ID" | awk 'NR > 1 {{print $1}}')
    job_memory=$(nvidia-smi -i "$CUDA_VISIBLE_DEVICES" --query-compute-apps=pid,used_memory --format=csv,noheader,nounits \\
        | awk -F', ' -v pids="$job_pids" 'BEGIN {{split(pids, p, " "); for (i in p) job[p[i]] = 1}} ($1 in job) {{total += $2}} END {{print total + 0}}')
    cpu_usec=$(awk '/^usage_usec/ {{print $2}}' "$job_cgroup/cpu.stat")
    now_ns=$(date +%s%N)
    cpu_cores=$(awk -v used=$((cpu_usec - previous_cpu_usec)) -v elapsed=$((now_ns - previous_ns)) 'BEGIN {{printf "%.3f", used * 1000 / elapsed}}')
    cpu_mem_mb=$(awk '/^anon / {{printf "%.1f", $2 / 1048576}}' "$job_cgroup/memory.stat")
    previous_cpu_usec=$cpu_usec
    previous_ns=$now_ns
    echo "$(date +%s), $sm_active, $job_memory, $cpu_cores, $cpu_mem_mb" >> "$usage_log"
    sample_count=$((sample_count + 1))
    # 30초(6개)마다 DB에 올림. 파이썬을 매번 띄우면 작업 CPU 사용량에 섞이기 때문
    if [ $((sample_count % 6)) -eq 0 ]; then
        record_samples
    fi
done &
monitor_pid=$!

# GPU 메모리 한도는 서버 task_prolog.sh가 정한 값 대신 이 작업에서 계산한 값을 씀 (MPS 클라이언트가 시작할 때 읽음)
export CUDA_MPS_PINNED_DEVICE_MEM_LIMIT="${{CUDA_VISIBLE_DEVICES}}={gpu_mem_limit_mb}M"
run_container {image} {command}
exit_code=$?

kill $monitor_pid
wait $monitor_pid 2>/dev/null
record_samples
# 작업 cgroup의 메모리 최고점 (Slurm이 --mem 한도를 적용하는 양). 다음 메모리 요청량의 기준
memory_peak_bytes=$(cat "$job_cgroup/memory.peak" 2>/dev/null)
{project_root}/.venv/bin/python3 -c "from common.slurm.job_records import record_job_end, record_memory_peak; record_job_end('$SLURM_JOB_ID', $exit_code); record_memory_peak('$SLURM_JOB_ID', '$memory_peak_bytes')"
if [ $exit_code -eq 0 ]; then
    {project_root}/.venv/bin/python3 -c "from common.slurm.job_history import record_job_usage; record_job_usage('{job_key}', '$SLURM_JOB_ID', $CUDA_MPS_ACTIVE_THREAD_PERCENTAGE)"
fi

exit $exit_code
"""


def build_sbatch_script(image, command, job_key, gres, cpus_per_task, mem, gpu_mem_limit_gb, qos, time):
    return SBATCH_TEMPLATE.format(
        project_root=PROJECT_ROOT,
        image=image,
        command=command,
        gres=gres,
        cpus_per_task=cpus_per_task,
        mem=mem,
        qos=qos,
        time=time,
        job_key=job_key,
        gpu_mem_limit_mb=int(gpu_mem_limit_gb * 1024),
    )
