FROM python:3.11-slim

WORKDIR /workspace

RUN python -m pip install --upgrade pip \
    && pip install gridfm-datakit==1.1.0 \
    && gridfm_datakit setup_pm

CMD ["bash"]
