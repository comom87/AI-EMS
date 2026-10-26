# 데이터 소스·모델이 자기 폴더에 둔 schema.sql을 DB에 적용함. 각 DAG가 시작할 때 부름
# (새 DB를 처음 띄울 때 자동 적용되는 것은 db/의 플랫폼 SQL뿐이라, 나머지는 여기서 적용)
# 모든 schema.sql은 여러 번 실행해도 안전하게 씀 (CREATE ... IF NOT EXISTS)
from sqlalchemy import create_engine

from common.config.settings import DATABASE_URL
from common.config.model_specs import fetch_model_dir


def apply_sql_file(path):
    # SQL 안의 %(format 함수 등)를 값 자리로 해석하지 않도록 드라이버에 그대로 보냄
    connection = create_engine(DATABASE_URL).raw_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(path.read_text())
        connection.commit()
    finally:
        connection.close()


def apply_model_schema(model_name):
    # sources/<기능>/schema.sql: 서빙이 예측을 저장하는 표. 없으면 건너뜀
    path = fetch_model_dir(model_name) / "schema.sql"
    if path.exists():
        apply_sql_file(path)
