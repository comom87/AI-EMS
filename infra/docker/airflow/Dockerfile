FROM apache/airflow:2.9.3-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends munge slurm-client \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir pandas==2.1.4 pyarrow==16.1.0 sqlalchemy==1.4.52 psycopg2-binary==2.9.9 "feast[postgres,redis]==0.49.0" mlflow==3.16.1 \
    && pip install --no-cache-dir protobuf==5.29.6
RUN pip install --no-cache-dir gridfm-datakit==1.1.0

# 저장소 경로(PYTHONPATH)와 DAG 폴더는 실행할 때 docker-compose.yml에서 정함
