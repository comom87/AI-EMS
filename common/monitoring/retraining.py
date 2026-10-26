# 재학습 판단: 학습을 다시 돌려도 되는지 확인하고 요청을 기록함 (<모델>_monitoring DAG가 부름). 테이블 정의는 db/model_checks.sql
from datetime import datetime, timedelta, timezone

import pandas as pd
from airflow.models import DagRun
from airflow.utils.state import DagRunState
from sqlalchemy import create_engine

from common.config.settings import DATABASE_URL


def check_training_availability(training_dag_id, cooldown_days):
    # 학습이 돌고 있거나 cooldown_days 안에 시작한 학습이 있으면 False
    cutoff = datetime.now(timezone.utc) - timedelta(days=cooldown_days)
    for run in DagRun.find(dag_id=training_dag_id):
        if run.state in (DagRunState.QUEUED, DagRunState.RUNNING):
            return False
        if run.start_date is not None and run.start_date > cutoff:
            return False
    return True


def record_retraining_request(model_name, reasons):
    pd.DataFrame(
        [{"model_name": model_name, "requested_at": datetime.now(timezone.utc), "reason": ",".join(reasons)}]
    ).to_sql("model_retraining_requests", create_engine(DATABASE_URL), schema="common", if_exists="append", index=False)
