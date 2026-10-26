# 공용 서빙 이미지: 학습 이미지(graphkit)에 Feast(온라인 저장소 읽기)를 더함. 실행 명령은 docker-compose.yml에서 정함
# 네트워크가 느려도 끝까지 받도록 pip 대기 시간과 재시도를 늘림
FROM aiems-graphkit

RUN pip install --no-cache-dir --timeout 120 --retries 10 "feast[redis,postgres]==0.49.0" psycopg2-binary==2.9.13
