# 재학습 요청 기록 (<모델>_monitoring 파이프라인의 request_retraining 단계가 부름). 학습을 다시 돌려도 되는지는 dags/operators/monitoring.py가 Airflow 실행 기록으로 확인함. 테이블 정의는 db/model_monitoring.sql
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine

from common.config.settings import DATABASE_URL


def record_retraining_request(model_name, reasons):
    pd.DataFrame(
        [{"model_name": model_name, "requested_at": datetime.now(timezone.utc), "reason": ",".join(reasons)}]
    ).to_sql("model_retraining_requests", create_engine(DATABASE_URL), schema="common", if_exists="append", index=False)
