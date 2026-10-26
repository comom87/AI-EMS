FROM python:3.11-slim

RUN pip install --no-cache-dir --timeout 120 --retries 10 mlflow==3.16.1 psycopg2-binary==2.9.13

# 실행 명령(DB 주소, 포트 등)은 docker-compose.yml에서 정함
