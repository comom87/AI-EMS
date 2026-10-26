-- 모델 점검 기록 (common 스키마, common/monitoring/performance.py와 common/monitoring/drift.py가 씀). 모든 모델이 같이 쓰고 model_name으로 나눔
-- 여러 번 실행해도 안전합니다 (이미 있는 스키마·테이블·인덱스는 건너뜀).

CREATE SCHEMA IF NOT EXISTS common;

-- 성능 점검 1회. 최근 예측 지표를 champion의 오프라인 평가값과 비교
CREATE TABLE IF NOT EXISTS common.model_performance_checks (
    model_name text NOT NULL,
    checked_at timestamptz NOT NULL,
    model_version integer NOT NULL,
    metric_name text NOT NULL,              -- 본 지표 이름 (model.yaml monitoring.metric_column, 예: pg_rmse_mw)
    recent_value double precision NOT NULL, -- 최근 window건을 aggregation으로 합친 값
    offline_baseline double precision NOT NULL,
    degraded boolean NOT NULL
);
CREATE INDEX IF NOT EXISTS model_performance_checks_model_time ON common.model_performance_checks (model_name, checked_at);

-- 분포 점검 1회. 최근 입력을 champion의 학습 데이터와 비교. 측정값 종류는 데이터 소스마다 달라서 measures에 모아 둠
CREATE TABLE IF NOT EXISTS common.model_drift_checks (
    model_name text NOT NULL,
    checked_at timestamptz NOT NULL,
    model_version integer NOT NULL,
    first_scenario bigint NOT NULL,
    last_scenario bigint NOT NULL,
    status text NOT NULL,                   -- stable, shifted, out_of_range
    measures jsonb NOT NULL                 -- 예: {"total_pd_psi": 0.03, "out_of_range_ratio": 0.0}
);
CREATE INDEX IF NOT EXISTS model_drift_checks_model_time ON common.model_drift_checks (model_name, checked_at);

-- 점검 결과로 학습 DAG에 재학습을 요청한(실행한) 기록
CREATE TABLE IF NOT EXISTS common.model_retraining_requests (
    model_name text NOT NULL,
    requested_at timestamptz NOT NULL,
    reason text NOT NULL                    -- performance_degradation, out_of_range, shifted (여러 개면 쉼표로)
);
