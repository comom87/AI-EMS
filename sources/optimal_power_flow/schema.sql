-- 모델 kpg193_realtime 전용 스키마. 예측 DAG(kpg193_realtime_predict)가 시작할 때마다 적용합니다. 여러 번 실행해도 안전합니다.
-- 공용 서빙 앱(serving/)이 예측할 때마다 씁니다. 모든 모델이 prediction_requests와 prediction_metrics를 같은 모양으로 가짐

CREATE SCHEMA IF NOT EXISTS kpg193_realtime;

-- 예측 요청 1건 (같은 시나리오·모델 버전이면 저장된 예측을 돌려주고 cached = true)
CREATE TABLE IF NOT EXISTS kpg193_realtime.prediction_requests (
    requested_at timestamptz NOT NULL,
    latency_ms double precision NOT NULL,
    scenario bigint NOT NULL,
    model_version integer NOT NULL,
    cached boolean NOT NULL
);
CREATE INDEX IF NOT EXISTS prediction_requests_time ON kpg193_realtime.prediction_requests (requested_at);

-- 시나리오 1개 예측의 요약 지표. 성능 점검은 model.yaml monitoring.metric_column(pg_rmse_mw)을 읽음
CREATE TABLE IF NOT EXISTS kpg193_realtime.prediction_metrics (
    scenario bigint NOT NULL,
    model_version integer NOT NULL,
    predicted_at timestamptz NOT NULL,
    pg_rmse_mw double precision,            -- 발전기 유효전력 RMSE (MW)
    vm_rmse_pu double precision,            -- 모선 전압 크기 RMSE (pu)
    mean_active_res_mw double precision,    -- 모선 유효전력 불평형 평균 (MW)
    mean_reactive_res_mvar double precision
);
CREATE INDEX IF NOT EXISTS prediction_metrics_version_time ON kpg193_realtime.prediction_metrics (model_version, predicted_at);
CREATE INDEX IF NOT EXISTS prediction_metrics_scenario ON kpg193_realtime.prediction_metrics (scenario, model_version);

-- 모선별 예측값과 실제값 (*_target)
CREATE TABLE IF NOT EXISTS kpg193_realtime.bus_predictions (
    scenario bigint NOT NULL,
    bus bigint NOT NULL,
    "Pd" real, "Qd" real,
    "Vm_min" real, "Vm_max" real, "Qg_min" real, "Qg_max" real,
    "Vm_target" real, "Va_target" real, "Pg_target" real, "Qg_target" real,
    "PQ" bigint, "PV" bigint, "REF" bigint,
    "Vm_pred" real, "Va_pred" real, "Pg_pred" real, "Qg_pred" real,
    active_res_mw real, reactive_res_mvar real, pbe_mva real,
    model_version integer NOT NULL,
    predicted_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS bus_predictions_scenario ON kpg193_realtime.bus_predictions (scenario, model_version);

-- 발전기별 예측값과 실제값 (운전 중인 발전기만)
CREATE TABLE IF NOT EXISTS kpg193_realtime.gen_predictions (
    scenario bigint NOT NULL,
    idx bigint NOT NULL,
    bus bigint,
    p_mw_target real, p_mw_pred real,
    min_p_mw real, max_p_mw real,
    cp0_eur real, cp1_eur_per_mw real, cp2_eur_per_mw2 real,
    model_version integer NOT NULL,
    predicted_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS gen_predictions_scenario ON kpg193_realtime.gen_predictions (scenario, model_version);
