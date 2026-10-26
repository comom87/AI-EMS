#!/bin/bash
echo "export CUDA_MPS_PIPE_DIRECTORY=/tmp/nvidia-mps-${CUDA_VISIBLE_DEVICES}"
echo "export CUDA_MPS_LOG_DIRECTORY=/tmp/nvidia-mps-log-${CUDA_VISIBLE_DEVICES}"
sm_mem_limit_gb=$((CUDA_MPS_ACTIVE_THREAD_PERCENTAGE * 24 / 100))
history_mem_limit_gb=${OGFM_GPU_MEM_GB:-0}
mem_limit_gb=$sm_mem_limit_gb
if [ "$history_mem_limit_gb" -gt "$mem_limit_gb" ]; then
    mem_limit_gb=$history_mem_limit_gb
fi
echo "export CUDA_MPS_PINNED_DEVICE_MEM_LIMIT=${CUDA_VISIBLE_DEVICES}=${mem_limit_gb}G"
