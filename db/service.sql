-- MLflow와 Airflow가 쓰는 데이터베이스. 같은 서버(compose의 postgres)에 aiems 데이터베이스와 나란히 둡니다.
-- 표는 각 서비스가 처음 뜰 때 스스로 만듭니다. 여러 번 실행해도 안전합니다 (psql 전용 문법 \gexec 사용).

SELECT 'CREATE DATABASE mlflow' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mlflow')\gexec
SELECT 'CREATE DATABASE airflow' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
