-- 모델 <모델 이름> 전용 스키마. 서빙 앱이 시작할 때마다 적용합니다. 여러 번 실행해도 안전하게 씁니다.
-- <모델 이름>을 model.yaml의 name으로 모두 바꿉니다.
-- prediction_requests와 prediction_metrics는 공용 서빙 앱(serving/)이 쓰므로 기본 열을 그대로 둡니다.

CREATE SCHEMA IF NOT EXISTS <모델 이름>;

-- 예측 요청 1건 (같은 시나리오·모델 버전이면 저장된 예측을 돌려주고 cached = true). 그대로 둠
CREATE TABLE IF NOT EXISTS <모델 이름>.prediction_requests (
    requested_at timestamptz NOT NULL,
    latency_ms double precision NOT NULL,
    scenario bigint NOT NULL,
    model_version integer NOT NULL,
    cached boolean NOT NULL
);
CREATE INDEX IF NOT EXISTS prediction_requests_time ON <모델 이름>.prediction_requests (requested_at);

-- 시나리오 1개 예측의 요약 지표. 앞의 세 열은 그대로 두고, 그 뒤에 serving/predictor.py의 calculate_metrics가 돌려주는 지표를 열로 추가
-- 성능 점검은 model.yaml monitoring.metric_column 열을 읽음
CREATE TABLE IF NOT EXISTS <모델 이름>.prediction_metrics (
    scenario bigint NOT NULL,
    model_version integer NOT NULL,
    predicted_at timestamptz NOT NULL,
    your_metric double precision
);
CREATE INDEX IF NOT EXISTS prediction_metrics_version_time ON <모델 이름>.prediction_metrics (model_version, predicted_at);
CREATE INDEX IF NOT EXISTS prediction_metrics_scenario ON <모델 이름>.prediction_metrics (scenario, model_version);

-- 예측값 표. serving/predictor.py의 PREDICTION_TABLES에 적은 표마다 하나씩 만듦
-- predict_scenario가 돌려주는 DataFrame의 열 + scenario, model_version, predicted_at
CREATE TABLE IF NOT EXISTS <모델 이름>.your_predictions (
    scenario bigint NOT NULL,
    your_key bigint NOT NULL,
    your_pred real,
    model_version integer NOT NULL,
    predicted_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS your_predictions_scenario ON <모델 이름>.your_predictions (scenario, model_version);
