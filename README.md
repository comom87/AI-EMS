# AI-EMS MLOps 플랫폼

여러 사용자가 머신러닝 모델을 만들어 운영에 올리고, 앱 개발자가 그 모델을 API로 쓰는 공용 플랫폼입니다.
운영자가 데이터와 플랫폼(학습·평가·승격·서빙·점검·재학습)을 맡고, 모델 개발자는 기능 폴더 하나(모델 하나)만 채우면 됩니다.

```
 앱 (내부망 프로그램) ── HTTP ──▶ 서빙 API (serving/)
                                     ▲
 모델 층   sources/<기능>/           ← 모델 개발자: 기능 하나 = 모델 하나
                                     ▲
 플랫폼 층 common/ serving/ dags/     ← 운영자: 모든 모델이 같이 쓰는 흐름
                                     ▲
 데이터 층 data/<소스>/               ← 운영자: 데이터 설명·등록·Feast 정의. 모델은 구독만
```

DAG는 아래 순서로 이어집니다. 앞 DAG가 끝나면 Airflow 신호(`common/workflows/datasets.py`)로 다음 DAG가 실행됩니다.

```
등록 <소스>_register (15분마다) ─▶ 예측 <모델>_predict ─▶ 점검 <모델>_monitoring ─▶ (나빠졌으면) 학습 <모델>_training
                                                                                    ▲
                                                                     정기 학습 (model.yaml의 schedule)
```

---

## 폴더와 파일

### 최상위

| 파일·폴더 | 내용 |
|---|---|
| `CLAUDE.md` | 코드 작성 원칙과 지켜야 할 것 (사람과 Claude 모두에게 적용) |
| `.env` | 설정과 비밀번호. git에 없고 운영자에게 받음 |
| `requirements.txt` | 호스트 가상환경(`.venv`)의 파이썬 라이브러리 |
| `common/` | 플랫폼 공용 코드 (모델과 무관) |
| `data/` | 데이터 소스별 폴더 |
| `sources/` | 기능별 폴더 (기능 하나 = 모델 하나) |
| `template/` | 새 기능 폴더의 복사 원본 |
| `serving/` | 모든 모델이 같이 쓰는 서빙 앱 |
| `dags/` | Airflow 진입점 |
| `db/` | 플랫폼 DB SQL |
| `infra/` | Docker·Slurm·Feast·Grafana 설정 |

### `common/` — 플랫폼 공용 코드

| 파일 | 하는 일 |
|---|---|
| **config/** | |
| `settings.py` | 저장소 경로, `.env` 값 읽기(`read_setting`), DB·MLflow 주소 |
| `source_specs.py` | `data/<소스>/source.yaml`을 찾고 검사하고, 소스의 `handler/` 코드를 불러옴 |
| `model_specs.py` | `sources/<기능>/models/model.yaml`을 찾고 검사하고(필수 칸, 이름 규칙, 학습 코드가 있는지), 기능 폴더 코드를 불러옴 |
| **workflows/** | DAG 종류마다 한 파일. 작업 순서와 내용 |
| `registration_pipeline.py` | 등록: 스키마 적용 → Feast 정의 적용(바뀌었을 때만) → 데이터 등록 → Redis 반영 |
| `training_pipeline.py` | 학습: 데이터 내보내기·검사 → Slurm 학습 → 후보·champion 평가 → 분포 기록 → 등록·승격 |
| `prediction_pipeline.py` | 예측: 예측 기록 표 적용 → 서빙 앱에 예측 요청 |
| `monitoring_pipeline.py` | 점검: 성능·분포 점검 → 필요하면 학습 DAG 실행 |
| `dag_settings.py` | DAG 공통 시작일과 DAG 이름 규칙 |
| `datasets.py` | DAG 사이 신호 (`aiems://data/<소스>`, `aiems://predictions/<모델>`) |
| **storage/** | |
| `schemas.py` | `schema.sql` 파일을 DB에 적용 |
| `feature_store.py` | 흩어진 Feast 정의 파일을 모아 적용, 데이터 소스별로 Redis에 반영 |
| **slurm/** | |
| `scheduling.py` | 작업 제출(`submit_training_job`)과 기다리기. 메모리 부족으로 실패하면 다음 요청량을 늘림 |
| `sbatch_script.py` | Slurm에 넘길 작업 스크립트: 컨테이너 실행, 5초마다 사용량 재기, 끝난 뒤 기록 |
| `resources.py` | 이전 사용량 × 1.3으로 GPU(MPS)·CPU·메모리 요청량과 QOS를 정함 |
| `job_records.py` | `common.slurm_jobs`: 작업 한 건의 제출·받은 몫·종료 상태·메모리 최고점 |
| `job_usage.py` | `common.slurm_job_usage`: 5초 간격 사용량 올리기와 p95 요약 |
| `job_history.py` | `common.slurm_job_history`: 같은 작업의 누적 최대 사용량 (다음 요청의 기준) |
| **model_registry/** | |
| `tracking.py` | MLflow run 찾기·태그·파일 올리기·내려받기 |
| `local_artifacts.py` | 학습 중 로컬에 잠깐 두는 run 파일(`mlruns/`, `work/`)의 위치와 정리 |
| `registry.py` | champion 찾기, 후보를 등록하고 champion보다 나으면 승격 |
| **monitoring/** | |
| `performance.py` | 최근 예측 지표를 오프라인 평가값과 비교해 성능 저하를 판정·기록 |
| `drift.py` | PSI 계산, 분포 판정(stable/shifted/out_of_range)과 기록. 무엇을 비교할지는 기능 폴더가 정함 |
| `retraining.py` | 재학습해도 되는지(학습 중이거나 cooldown 기간이면 안 됨) 확인하고 요청을 기록 |

### `data/<소스>/` — 데이터 하나에 관한 것 전부 (예: `kpg193/`)

| 파일 | 하는 일 |
|---|---|
| `source.yaml` | 이름·담당자·설명·등록 주기·재시도 횟수. 이 파일만 있으면 등록 DAG가 생김 |
| `schema.sql` | 소스 이름과 같은 DB 스키마와 인덱스. 등록 DAG가 시작할 때마다 적용 |
| `configs/` | datakit 설정과 계통 파일 |
| `feature_store.py` | 이 데이터가 제공하는 Feast 엔티티·피처 뷰 (이름 앞에 소스 이름, 예: `kpg193_bus`) |
| `handler/ingest.py` | 등록 코드: `apply_source_schema(source)`, `register_new_data(source)` (등록 DAG가 부름) |
| `handler/datakit.py` | datakit 컨테이너로 데이터 생성·검증·통계 |
| `files/` | 실제 데이터 파일 (datakit 결과, 모델이 학습용으로 내보낸 데이터). git에서 빠짐 |

### `sources/<기능>/` — 기능 하나와 그 모델 (예: `optimal_power_flow/` = 최적 조류 예측, 모델 `kpg193_realtime`)

| 파일 | 실행 위치 | 하는 일 |
|---|---|---|
| `models/model.yaml` | — | 모델 명세: 이름, 구독 데이터, 학습·평가·점검·분포 설정 |
| `models/configs/` | — | 학습 설정 |
| `schema.sql` | — | 예측 기록 표 (`prediction_requests`, `prediction_metrics`, 예측값 표). 예측 DAG가 적용 |
| `feature_store.py` | Feast | 서빙 때 읽는 피처 묶음 (FeatureService) |
| `training/data.py` | Airflow | 학습 데이터 내보내기·검사·설명 |
| `training/jobs.py` | Airflow | Slurm 학습·평가 작업 명령 만들기 |
| `training/split_scenarios.py` | 학습 컨테이너 | 학습·검증·시험 나누기 (graphkit용 torch 파일) |
| `training/publish_results.py` | 학습 컨테이너 | graphkit이 로컬에 쓴 지표·파일을 MLflow로 올리기 |
| `serving/predictor.py` | 서빙 | 모델 불러오기·예측·지표·응답 형식 |
| `serving/inputs.py` | 서빙 | Redis에서 최신 입력 읽기 |
| `monitoring/input_drift.py` | Airflow | 무엇을 비교할지 (총 부하, 정지 수, 모선별 부하) |
| `docker/` | — | (필요할 때만) 다른 라이브러리가 필요한 모델의 전용 학습 이미지 |

### `template/` — 새 기능 폴더의 복사 원본

`models/model.yaml` 틀과 칸 설명, 플랫폼이 부르는 함수의 빈 틀(`training/`, `serving/`, `monitoring/`), `schema.sql`·`feature_store.py` 틀이 있습니다.
함수마다 주석에 언제 불리는지, 무엇을 돌려줘야 하는지가 적혀 있습니다. 쓰는 방법은 아래 [새 모델 추가하기](#5-새-모델기능-추가하기)를 보세요.

### `serving/` — 공용 서빙 앱

| 파일 | 하는 일 |
|---|---|
| `app.py` | FastAPI 앱: `POST /models/<모델>/predict`, `GET /models/<모델>`, `GET /models`, `GET /health` |
| `models.py` | `serving/predictor.py`가 있는 모델 목록을 불러오고, 1분마다 champion이 바뀌었는지 보고 바꿔 끼움 |
| `predictions.py` | 예측·요청 기록을 모델 스키마에 저장하고 다시 읽음 |

### `dags/` — Airflow 진입점

DAG 종류마다 한 파일이라 한 파일의 오류가 다른 종류의 DAG를 멈추지 않습니다. 여기서는 `common/workflows/`의 함수를 부르기만 합니다.

| 파일 | 만드는 DAG | 언제 도나 |
|---|---|---|
| `registration_dags.py` | `<소스>_register` (예: `kpg193_register`) | `source.yaml`의 `schedule` (kpg193: 15분마다) |
| `training_dags.py` | `<모델>_training` | `model.yaml`의 `training.schedule`, 점검 DAG의 재학습 요청, 수동 실행 |
| `prediction_dags.py` | `<모델>_predict` | 구독하는 소스의 등록이 끝날 때마다 |
| `monitoring_dags.py` | `<모델>_monitoring` | 그 모델의 예측이 저장될 때마다 |
| `platform_dags.py` | `platform_slurm_job_records` | 15분마다 끝난 Slurm 작업의 상태를 채움 |

### `db/` — 플랫폼 DB SQL

새 DB를 처음 띄울 때 이름순으로 자동 실행됩니다. 데이터 소스와 모델의 SQL은 각자 폴더의 `schema.sql`에 둡니다.

| 파일 | 내용 |
|---|---|
| `service.sql` | MLflow·Airflow가 쓸 데이터베이스 |
| `slurm_jobs.sql` | `common` 스키마의 Slurm 작업 기록 표 3개 |
| `model_checks.sql` | `common` 스키마의 성능 점검·분포 점검·재학습 요청 표 |

DB 스키마는 다음과 같습니다.

| 스키마 | 내용 |
|---|---|
| `common` | Slurm 작업 기록, 모델 점검 기록 |
| `kpg193` | 원본 시나리오(9,994개)와 실시간 데이터(`*_realtime`, 등록 DAG가 하나씩 쌓음) |
| `kpg193_realtime` | 모델 `kpg193_realtime`의 예측 요청·예측값·예측 지표 |

### `infra/` — 서비스와 서버 설정

| 파일·폴더 | 내용 |
|---|---|
| `docker-compose.yml` | 서비스: `postgres`, `redis`, `mlflow`, `airflow`, `serving`, `grafana`. 도구: `graphkit`, `datakit` |
| `.env` | 루트 `.env`를 가리키는 링크 (compose도 같은 설정을 읽음) |
| `docker/` | 공용 이미지: `graphkit`(학습, MLflow 로거 패치 포함), `serving`, `airflow`, `mlflow`, `datakit` |
| `slurm/common.sh` | 작업 스크립트가 쓰는 컨테이너 실행 함수 (Apptainer SIF) |
| `slurm/etc/` | 서버 Slurm·MPS 설정 사본 (참고용) |
| `slurm/sif/` | 학습용 SIF 이미지 (git 제외) |
| `slurm/logs/` | Slurm 작업 로그 (git 제외) |
| `feast/feature_store.yaml` | Feast 프로젝트 `aiems` 설정 (Redis 키는 하루 뒤 지워짐) |
| `grafana/` | 데이터 소스 설정과 대시보드 JSON(`dashboards/aiems.json`). 대시보드는 화면이 아니라 이 파일을 고침 |

---

## 사용법

명령은 모두 저장소 루트(`~/projects/ai-ems`)에서 실행합니다.

### 1. 처음 설정

```bash
git clone https://github.com/comom87/AI-EMS.git ~/projects/ai-ems
```

운영자에게 `.env`를 받아 저장소 루트에 두고, 나만 읽을 수 있게 권한을 바꿉니다.

```bash
chmod 600 .env
```

가상환경은 반드시 저장소 안 `.venv`에 만듭니다. Slurm 작업 스크립트가 끝날 때 `.venv/bin/python3`로 사용량을 기록합니다. 서버의 기본 `python3`는 3.11이라 3.12를 직접 지정합니다.

```bash
/usr/bin/python3.12 -m venv .venv && .venv/bin/pip install -r requirements.txt
```

### 2. 서비스 띄우기와 접속

운영자가 한 번 띄워 두면 모두가 같이 씁니다.

```bash
docker compose -f infra/docker-compose.yml up -d
```

`<서버>`는 서버의 내부망 주소입니다. 포트는 `.env`의 `AIEMS_*_PORT`로 정하고, 계정은 운영자에게 받습니다.

| 서비스 | 주소 | 로그인 |
|---|---|---|
| Airflow | `http://<서버>:8082` | 운영자에게 받음 |
| Grafana (대시보드 `AI-EMS MLOps`) | `http://<서버>:3001` | 운영자에게 받음 (비밀번호는 `.env`의 `AIEMS_GRAFANA_PASSWORD`) |
| MLflow | `http://<서버>:5002` | 없음 |
| 서빙 API | `http://<서버>:8001/docs` | 없음 |
| PostgreSQL | `<서버>:5434` | `.env`의 `AIEMS_DB_USER` / `AIEMS_DB_PASSWORD` |
| Redis | 서버 안에서만 (`127.0.0.1:6380`) | 없음 |

### 3. 이미지 만들기

서비스 이미지는 compose로 빌드합니다.

```bash
docker compose -f infra/docker-compose.yml build airflow serving mlflow
```

학습 이미지는 Slurm이 Apptainer로 실행하므로, Docker 이미지를 만든 뒤 SIF로 바꿔 `infra/slurm/sif/<이미지 이름>.sif`에 둡니다. 파일 이름이 `model.yaml`의 `training.image`와 같아야 합니다. 홈 디스크 용량이 모자라면 `APPTAINER_TMPDIR`·`APPTAINER_CACHEDIR`을 큰 디스크로 지정합니다.

```bash
docker compose -f infra/docker-compose.yml --profile build build graphkit
```

```bash
apptainer build infra/slurm/sif/aiems-graphkit.sif docker-daemon://aiems-graphkit:latest
```

### 4. 데이터 소스 추가하기 (운영자)

1. `data/<소스>/`를 만들고 `source.yaml`, `schema.sql`, `feature_store.py`, `handler/ingest.py`를 씁니다. `kpg193/`을 참고합니다.
   - `ingest.py`에는 `apply_source_schema(source)`와 `register_new_data(source)`가 있어야 합니다.
   - Feast 이름은 앞에 소스 이름을 붙여 다른 소스와 겹치지 않게 합니다.
2. 5분 안에 Airflow가 `<소스>_register` DAG를 만듭니다. Feast 정의는 그 DAG가 처음 돌 때 적용됩니다.
3. datakit으로 데이터를 만들 때는 `data/<소스>/handler/datakit.py`의 `generate_dataset(source, config_path)`를 씁니다. 결과는 `data/<소스>/files/`에 저장됩니다.

### 5. 새 모델(기능) 추가하기 (모델 개발자)

1. **폴더 복사**
   ```bash
   cp -r template sources/<기능 이름>
   ```
2. **모델 명세:** `models/model.yaml`을 채웁니다.
   - `name`은 모델 이름입니다. MLflow, DB 스키마, DAG 이름에 같이 쓰입니다.
   - `data_source`는 구독할 데이터입니다.
   - 나머지 칸의 뜻은 주석에 있습니다.
3. **이름 바꾸기:** 파일 안의 `your_function`은 기능 폴더 이름으로, `<모델 이름>`은 `name`으로 바꿉니다.
4. **함수 채우기:** 각 파일의 `NotImplementedError` 자리를 채웁니다. 함수 이름과 인자는 바꾸지 않습니다. 플랫폼이 이 이름으로 부릅니다.

   | 파일 | 채울 함수 | 지킬 것 |
   |---|---|---|
   | `training/data.py` | `export_dataset`, `validate_dataset`, `describe_dataset` | 데이터는 `data/<소스>/files/<모델 이름>/` 아래에 저장. 검사에 실패하면 예외를 냄 |
   | `training/jobs.py` | `build_train_job`, `build_evaluate_job` | `image`, `command`, `job_key`를 담은 dict를 돌려줌 |
   | `training/train.py` | `fit_model`, `evaluate_model` (컨테이너 안) | 학습 run 이름은 `run_name`, 평가 run에는 `evaluation.metric` 지표를 기록 |
   | `serving/predictor.py` | `load_model`, `fetch_latest_scenario`, `predict_scenario`, `calculate_metrics`, `format_predictions`, `PREDICTION_TABLES` | 지표에 `monitoring.metric_column`이 들어 있어야 함 |
   | `monitoring/input_drift.py` | `measure_recent_drift`, `measure_training_drift` | measures에 `*_psi` 값과 `out_of_range_ratio`를 넣음 |
   | `schema.sql`, `feature_store.py` | 예측 기록 표, 피처 묶음 | 기본 열(`scenario`, `model_version`, `predicted_at`)은 그대로 둠 |

5. **라이브러리:**
   - 학습: 공용 graphkit 이미지에 없는 라이브러리가 필요하면 `sources/<기능>/docker/`에 전용 이미지를 만들고 3번처럼 SIF로 바꿉니다.
   - 서빙: 이미지는 모든 모델이 같이 쓰므로, 필요한 라이브러리는 운영자에게 `infra/docker/serving/Dockerfile`에 넣어 달라고 요청합니다.
6. **반영:** 5분 안에 Airflow가 `<모델>_training`, `<모델>_predict`, `<모델>_monitoring` DAG를 만듭니다. 오류가 없는지는 [8번](#8-바꾼-뒤-확인하기)으로 확인합니다.

### 6. 학습 돌리기

- Airflow 화면에서 `<모델>_training`을 켜거나 ▶(Trigger)로 실행합니다.
- 실행할 때 `config_path`(학습 설정)와 `data_dir`(학습 데이터 폴더)을 바꿀 수 있습니다. `data_dir`을 다른 폴더로 주면 그 데이터를 그대로 학습합니다.
- 첫 학습은 비교할 champion이 없어서 바로 champion이 됩니다. 그다음부터는 같은 시험 데이터로 평가한 champion보다 나을 때만 승격합니다.
- 결과는 MLflow(실험 = 모델 이름)와 Grafana에서 봅니다. Slurm 로그는 `infra/slurm/logs/model-train-<작업 번호>.out`에 있습니다.

DAG가 꺼진 동안 밀린 정기 실행이 있으면, DAG를 켤 때 그 실행이 바로 시작됩니다. 이때 ▶까지 누르면 학습이 두 번 돕니다.

### 7. 예측·점검 켜기

champion이 생긴 뒤에 켭니다.

1. 서빙 컨테이너를 다시 띄웁니다. 서빙할 모델 목록은 시작할 때만 읽습니다. champion이 바뀌는 것은 1분 안에 알아서 반영됩니다.
   ```bash
   docker compose -f infra/docker-compose.yml restart serving
   ```
2. Airflow에서 `<모델>_predict`와 `<모델>_monitoring`을 켭니다.
3. 직접 예측해 볼 수도 있습니다.
   ```bash
   curl -X POST http://<서버>:8001/models/<모델 이름>/predict
   ```

### 8. 바꾼 뒤 확인하기

`common/`, `dags/`, 기능 폴더의 Airflow용 파일을 고친 뒤에는 꼭 DAG 불러오기 오류를 확인합니다. 오류가 나면 그 파일의 DAG가 모두 사라집니다.

```bash
docker compose -f infra/docker-compose.yml exec airflow airflow dags list-import-errors
```

Feast 정의를 바로 적용하려면(등록 DAG를 기다리지 않고):

```bash
docker compose -f infra/docker-compose.yml exec airflow python3 -c 'from common.storage.feature_store import apply_feature_definitions; apply_feature_definitions()'
```

`db/`의 SQL을 고친 뒤에는 직접 적용합니다. 모든 SQL은 여러 번 실행해도 안전합니다. 데이터 소스와 모델의 `schema.sql`은 DAG가 시작할 때 알아서 적용합니다.

```bash
docker exec -i aiems-postgres-1 sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < db/slurm_jobs.sql
```
