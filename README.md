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

데이터 등록과 모델의 DAG는 모두 **파이프라인 YAML**로 조립됩니다. `data/<소스>/pipelines/<이름>.yaml`은 DAG `<소스>_<이름>`, `sources/<기능>/pipelines/<이름>.yaml`은 DAG `<모델>_<이름>`이 되고, 단계·순서·주기를 YAML에서 바꾸거나 새 파이프라인을 더할 수 있습니다.
기본 흐름은 아래와 같습니다. 예측은 외부 시스템이 서빙 API를 부를 때 일어나고, 서빙 앱이 새 예측을 저장하면 Airflow 신호(`aiems://predictions/<모델>`)를 보내 점검 DAG가 실행됩니다.

```
등록 <소스>_register (15분마다) ─▶ 피처 저장소(Redis)에 최신 입력
외부 시스템 ─▶ 서빙 POST /models/<모델>/predict ─▶ 예측 저장 ─▶ 점검 <모델>_monitoring ─▶ (나빠졌으면) 학습 <모델>_training
                                                                                                  ▲
                                                                                   정기 학습 (pipelines/training.yaml의 schedule)
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
| `dags/` | Airflow에 관한 것 전부: 진입점, 파이프라인 YAML 조립, operator |
| `db/` | 플랫폼 DB SQL |
| `infra/` | Docker·Slurm·Feast·Grafana 설정 |

### `common/` — 플랫폼 공용 코드

Airflow와 무관한 일반 함수입니다. Airflow에 연결하는 코드는 `dags/`에 있습니다.

| 파일 | 하는 일 |
|---|---|
| **config/** | |
| `settings.py` | 저장소 경로, `.env` 값 읽기(`read_setting`), DB·MLflow 주소 |
| `source_specs.py` | 데이터 소스(`data/` 아래 폴더, 폴더 이름 = 소스 이름)를 찾고, 소스의 `handler/` 코드를 불러옴 |
| `model_specs.py` | `sources/<기능>/models/model.yaml`을 찾고 검사하고(필수 칸, 이름 규칙), 기능 폴더 코드를 불러옴 |
| **storage/** | |
| `schemas.py` | 데이터 소스·모델의 `schema.sql`을 DB에 적용 |
| `feature_store.py` | 흩어진 Feast 정의 파일을 모아 적용, 데이터 소스별로 Redis에 반영 |
| **slurm/** | |
| `scheduling.py` | 작업 제출(`submit_training_job`)과 기다리기. 메모리 부족으로 실패하면 다음 요청량을 늘림 |
| `sbatch_script.py` | Slurm에 넘길 작업 스크립트: 컨테이너 실행, 5초마다 사용량 재기, 끝난 뒤 기록 |
| `resources.py` | 이전 사용량 × 1.3으로 GPU(MPS)·CPU·메모리 요청량과 QOS를 정함 |
| `job_records.py` | `common.slurm_jobs`: 작업 한 건의 제출·받은 몫·종료 상태·메모리 최고점 |
| `job_usage.py` | `common.slurm_job_usage`: 5초 간격 사용량 올리기와 p95 요약 |
| `job_history.py` | `common.slurm_job_history`: 같은 작업의 누적 최대 사용량 (다음 요청의 기준) |
| `pipelines/slurm_job_records.yaml` | 플랫폼 파이프라인 → DAG `platform_slurm_job_records`: 15분마다 종료 기록이 빠진 Slurm 작업의 상태를 `sacct`로 채움 |
| **model_registry/** | |
| `tracking.py` | MLflow run 찾기·태그·파일 올리기·내려받기 |
| `local_artifacts.py` | 학습 중 로컬에 잠깐 두는 run 파일(`mlruns/`, `tmp/`)의 위치와 정리 |
| `registry.py` | champion 찾기, 후보를 등록하고 champion보다 나으면 승격 |
| **monitoring/** | |
| `performance.py` | 최근 예측 지표를 오프라인 평가값과 비교해 성능 저하를 판정·기록 |
| `drift.py` | PSI 계산, 분포 판정(stable/shifted/out_of_range)과 기록. 무엇을 비교할지는 기능 폴더가 정함 |
| `retraining.py` | 재학습 요청 기록 (재학습해도 되는지는 `dags/operators/monitoring.py`가 Airflow 실행 기록으로 확인) |

### `data/<소스>/` — 데이터 하나에 관한 것 전부 (예: `kpg193/`)

| 파일 | 하는 일 |
|---|---|
| `pipelines/register.yaml` | 등록 파이프라인 → DAG `<소스>_register`: 스키마 적용 → Feast 정의 적용(바뀌었을 때만) → 데이터 등록 → Redis 반영. 주기·재시도도 여기서 정함 |
| `storage/schema.sql` | 소스 이름과 같은 DB 스키마와 인덱스. 등록 DAG가 시작할 때마다 적용 |
| `configs/` | datakit 설정과 계통 파일 |
| `storage/feature_store.py` | 이 데이터가 제공하는 Feast 엔티티·피처 뷰 (이름 앞에 소스 이름, 예: `kpg193_bus`) |
| `handler/ingest.py` | 등록 코드: `register_new_data(spec)` (등록 파이프라인의 `function` 작업이 부름), `register_dataset(source, data_dir)` (원본을 한 번에 넣을 때 직접 실행) |
| `handler/datakit.py` | datakit 컨테이너로 데이터 생성·검증·통계 |
| `files/` | 실제 데이터 파일 (datakit 결과, 모델이 학습용으로 내보낸 데이터). git에서 빠짐 |

### `sources/<기능>/` — 기능 하나와 그 모델 (예: `optimal_power_flow/` = 최적 조류 예측. 기능 폴더 이름이 곧 모델 이름)

| 파일 | 실행 위치 | 하는 일 |
|---|---|---|
| `models/model.yaml` | — | 모델 명세: 이름, 구독 데이터, 학습·평가·점검·분포 설정 |
| `models/configs/` | — | 학습 설정 |
| `pipelines/*.yaml` | — | 파이프라인 정의. 파일마다 DAG 하나 (`training.yaml` → `<모델>_training`, `monitoring.yaml` → `<모델>_monitoring`) |
| `storage/schema.sql` | — | 예측 기록 표 (`prediction_requests`, `prediction_metrics`, 예측값 표). 서빙 앱이 시작할 때 적용 |
| `storage/feature_store.py` | Feast | 서빙 때 읽는 피처 묶음 (FeatureService) |
| `training/dataset_preparation.py` | Airflow | 학습 데이터 수집·전처리·검사·설명 |
| `training/model_train.py` | Airflow | 모델 학습·평가를 Slurm 작업으로 만들기 |
| `training/split_scenarios.py` | 학습 컨테이너 | 학습·검증·시험 나누기 (graphkit용 torch 파일) |
| `training/publish_results.py` | 학습 컨테이너 | graphkit이 로컬에 쓴 지표·파일을 MLflow로 올리기 |
| `serving/predictor.py` | 서빙 | 모델 불러오기·예측·지표·응답 형식 |
| `serving/serving_features.py` | 서빙 | Feast 온라인 저장소(Redis)에서 최신 입력 읽기 |
| `monitoring/input_drift.py` | Airflow | 무엇을 비교할지 (총 부하, 정지 수, 모선별 부하) |
| `docker/` | — | (필요할 때만) 다른 라이브러리가 필요한 모델의 전용 학습 이미지 |

### `template/` — 새 기능 폴더의 복사 원본

`models/model.yaml` 틀과 칸 설명, 기본 파이프라인 YAML(`pipelines/`), 파이프라인이 부르는 함수의 빈 틀(`training/`, `serving/`, `monitoring/`), `storage/`(`schema.sql`·`feature_store.py`) 틀이 있습니다.
함수마다 주석에 언제 불리는지, 무엇을 돌려줘야 하는지가 적혀 있습니다. 쓰는 방법은 아래 [새 모델 추가하기](#5-새-모델기능-추가하기)를 보세요.

### `serving/` — 공용 서빙 앱

| 파일 | 하는 일 |
|---|---|
| `app.py` | FastAPI 앱: `POST /models/<모델>/predict`, `GET /models/<모델>`, `GET /models`, `GET /health`. 시작할 때 모델마다 예측 기록 표(`storage/schema.sql`)를 적용 |
| `models.py` | `serving/predictor.py`가 있는 모델 목록을 불러오고, 1분마다 champion이 바뀌었는지 보고 바꿔 끼움 |
| `predictions.py` | 예측·요청 기록을 모델 스키마에 저장하고 다시 읽음 |
| `signals.py` | 새 예측을 저장하면 Airflow REST API로 `aiems://predictions/<모델>` 신호를 보냄 (계정은 `.env`의 `AIEMS_AIRFLOW_API_USER`·`_PASSWORD`). 실패하면 `/health`의 `signal_error`에 보임 |

### `dags/` — Airflow에 관한 것 전부

모든 DAG는 파이프라인 YAML로 조립되고, Airflow가 읽는 파일은 진입점 `pipeline_dags.py` 하나입니다. 나머지 파일과 `operators/` 폴더는 `.airflowignore`로 DAG 파일로 읽지 않게 합니다.
YAML 하나에 실수가 있으면 모든 DAG가 함께 멈추므로, 고친 뒤에는 [9번](#9-바꾼-뒤-확인하기)으로 꼭 확인합니다.

| 파일 | 하는 일 |
|---|---|
| `pipeline_dags.py` | 진입점. 플랫폼·데이터 소스·모델의 `pipelines/*.yaml`을 모두 찾아 DAG를 만듦 |
| `pipeline_builder.py` | YAML을 읽고 검사해서 DAG를 조립. `schedule`·`outlets`의 `predictions`를 DAG 사이 신호(Airflow Dataset, 주소는 `common/config/signals.py`)로 바꿈 |
| `operators/` | YAML에서 쓰는 operator(단계). `__init__.py`에 이름 → 함수 목록, 단계 함수는 쓰이는 곳별 파일: `general.py`(function, apply_schema), `features.py`(데이터 소스의 Feast), `training.py`, `prediction.py`, `monitoring.py`, `slurm_records.py`(플랫폼) |
| `dag_settings.py` | DAG 공통 시작일과 DAG 이름 규칙 (`<주인>_<파일 이름>`) |
| `.airflowignore` | Airflow가 DAG 파일로 읽지 않을 파일 목록 |

| YAML 위치 | 만드는 DAG | 예 |
|---|---|---|
| `common/<영역>/pipelines/<이름>.yaml` | `platform_<이름>` | `platform_slurm_job_records` |
| `data/<소스>/pipelines/<이름>.yaml` | `<소스>_<이름>` | `kpg193_register` |
| `sources/<기능>/pipelines/<이름>.yaml` | `<모델>_<이름>` | `optimal_power_flow_training`, `_monitoring` |

### `db/` — 플랫폼 DB SQL

새 DB를 처음 띄울 때 이름순으로 자동 실행됩니다. 데이터 소스와 모델의 SQL은 각자 폴더의 `storage/schema.sql`에 둡니다.

| 파일 | 내용 |
|---|---|
| `service.sql` | MLflow·Airflow가 쓸 데이터베이스 |
| `slurm_jobs.sql` | `common` 스키마의 Slurm 작업 기록 표 3개 |
| `model_monitoring.sql` | `common` 스키마의 성능 점검·분포 점검·재학습 요청 표 |

DB 스키마는 다음과 같습니다.

| 스키마 | 내용 |
|---|---|
| `common` | Slurm 작업 기록, 모델 점검 기록 |
| `kpg193` | 원본 시나리오(9,994개)와 실시간 데이터(`*_realtime`, 등록 DAG가 하나씩 쌓음) |
| `optimal_power_flow` | 모델 `optimal_power_flow`의 예측 요청·예측값·예측 지표 |

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

1. `data/<소스>/` 폴더를 만들고(폴더 이름이 소스 이름, 영문 소문자·숫자·밑줄) `pipelines/register.yaml`, `storage/schema.sql`, `storage/feature_store.py`, `handler/ingest.py`를 씁니다. `kpg193/`을 참고합니다.
   - `register.yaml`의 `dag:`에 설명(`description`)과 담당자(`owner`)를 적습니다.
   - `register.yaml`의 등록 단계(`operator: function`)가 부르는 함수(예: `ingest.register_new_data`)는 `handler/`에서 찾고, 첫 인자로 `spec`(`{name: <소스>}`)을 받습니다.
   - Feast 이름은 앞에 소스 이름을 붙여 다른 소스와 겹치지 않게 합니다.
2. 5분 안에 Airflow가 `<소스>_register` DAG를 만듭니다. Feast 정의는 그 DAG의 `apply_features` 작업이 처음 돌 때 적용됩니다.
3. datakit으로 데이터를 만들 때는 `data/<소스>/handler/datakit.py`의 `generate_dataset(source, config_path)`를 씁니다. 결과는 `data/<소스>/files/`에 저장됩니다.

### 5. 새 모델(기능) 추가하기 (모델 개발자)

1. **폴더 복사**
   ```bash
   cp -r template sources/<기능 이름>
   ```
   기능 폴더 이름이 곧 모델 이름입니다. MLflow 실험·등록 모델, DB 스키마, DAG 이름 앞부분, 서빙 주소(`/models/<기능 이름>/predict`)에 같이 쓰이므로 영문 소문자·숫자·밑줄로 짓습니다.
2. **모델 명세:** `models/model.yaml`을 채웁니다.
   - `data_source`는 구독할 데이터입니다.
   - 나머지 칸의 뜻은 주석에 있습니다.
3. **이름 바꾸기:** 파일 안의 `your_function`은 기능 폴더 이름으로, `<모델 이름>`은 모두 기능 폴더 이름으로 바꿉니다.
4. **파이프라인 정하기:** `pipelines/`의 기본 YAML 3개를 그대로 쓰거나, 단계·순서·주기를 바꿉니다 ([6번](#6-파이프라인-yaml-쓰기)).
5. **함수 채우기:** 각 파일의 `NotImplementedError` 자리를 채웁니다. 기본 파이프라인이 아래 이름과 인자로 부르므로, YAML을 바꾸지 않았다면 이름과 인자를 그대로 둡니다.

   | 파일 | 채울 함수 | 지킬 것 |
   |---|---|---|
   | `training/dataset_preparation.py` | `export_dataset`, `validate_dataset`, `describe_dataset` | 데이터는 `data/<소스>/files/<모델 이름>/` 아래에 저장. 검사에 실패하면 예외를 냄 |
   | `training/model_train.py` | `build_train_job`, `build_evaluate_job` | `image`, `command`, `job_key`를 담은 dict를 돌려줌 |
   | `training/train.py` | `fit_model`, `evaluate_model` (컨테이너 안) | 학습 run 이름은 `run_name`, 평가 run에는 `evaluation.metric` 지표를 기록 |
   | `serving/predictor.py` | `load_model`, `fetch_latest_scenario`, `predict_scenario`, `calculate_metrics`, `format_predictions`, `PREDICTION_TABLES` | 지표에 `monitoring.metric_column`이 들어 있어야 함 |
   | `monitoring/input_drift.py` | `measure_recent_drift`, `measure_new_data_drift`, `measure_champion_data_drift` | measures에 `*_psi` 값과 `out_of_range_ratio`를 넣음 |
   | `storage/schema.sql`, `storage/feature_store.py` | 예측 기록 표, 피처 묶음 | 기본 열(`scenario`, `model_version`, `predicted_at`)은 그대로 둠 |

6. **라이브러리:**
   - 학습: 공용 graphkit 이미지에 없는 라이브러리가 필요하면 `sources/<기능>/docker/`에 전용 이미지를 만들고 3번처럼 SIF로 바꿉니다.
   - 서빙: 이미지는 모든 모델이 같이 쓰므로, 필요한 라이브러리는 운영자에게 `infra/docker/serving/Dockerfile`에 넣어 달라고 요청합니다.
7. **반영:** 5분 안에 Airflow가 `pipelines/`의 YAML마다 DAG를 만듭니다. YAML 실수(없는 operator·함수·작업 이름)는 불러오기 오류로 보이니 [9번](#9-바꾼-뒤-확인하기)으로 확인합니다.

### 6. 파이프라인 YAML 쓰기

`sources/<기능>/pipelines/<이름>.yaml` 하나가 DAG `<모델>_<이름>` 하나입니다(데이터 소스는 `data/<소스>/pipelines/` → `<소스>_<이름>`). 기본 YAML을 고치거나, 새 이름으로 파일을 더 만들면 새 DAG가 생깁니다.

```yaml
dag:
  description: 한 줄 설명
  schedule: "@monthly"                       # cron·프리셋, predictions(서빙 앱이 이 모델의 새 예측을 저장할 때마다)
  owner: your_github_account                 # 담당자 (Airflow 화면의 Owner 칸). 모델은 생략하면 model.yaml의 owner
  retries: 0                                 # 모든 작업의 기본 재시도 횟수 (작업마다 retries로 바꿀 수 있음)
  params:                                    # Airflow 화면에서 실행할 때 바꿀 수 있는 값. 기본값에 {{ spec.<model.yaml 칸> }}을 쓸 수 있음
    data_dir: "{{ spec.training.data_dir }}"

tasks:
  <작업 이름>:
    operator: function                       # 아래 operator 중 하나
    args:                                    # operator에 넘길 값. Airflow 템플릿을 씀 ({{ params.data_dir }}, {{ ti.xcom_pull(task_ids='앞 작업') }})
      function: training.dataset_preparation.export_dataset
    dependencies: [<앞 작업 이름>]            # 이 작업들이 끝난 뒤 실행
    trigger_rule: all_done                   # (선택) 앞 작업이 실패해도 실행하려면 all_done
    retries: 3                               # (선택)
    outlets: [predictions]                   # (선택) 끝나면 predictions 신호를 냄 (모델 파이프라인만. schedule: predictions인 파이프라인이 실행됨)
```

| operator | 하는 일 | args |
|---|---|---|
| `function` | 기능 폴더(데이터 소스는 `handler/`)의 함수를 `(spec, **args)`로 부름. 돌려준 값은 뒤 작업이 `ti.xcom_pull`로 받음 | `function`(예: `training.dataset_preparation.export_dataset`, `ingest.register_new_data`), 그 밖의 값은 함수 인자 |
| `slurm_train` | 작업 내용 함수로 Slurm 학습 제출, 학습 run에 데이터 설명 태그·학습 설정 기록 | `job`, `config_path`, `data_dir`, (선택) `describe` |
| `slurm_evaluate` | 후보(`candidate`) 또는 champion(`champion`)을 같은 시험 데이터로 Slurm 평가 | `job`, `model`, `config_path`, `data_dir` |
| `measure_new_data_drift` | 학습 데이터 가운데 지난 학습 뒤 새로 쌓인 데이터를 그 전 데이터와 비교해 결과를 돌려줌 (XCom) | `data_dir` |
| `record_champion_data_drift` | 운영 모델(champion)과 후보 모델의 학습 데이터 차이를 학습 run에 기록 | `data_dir` |
| `delete_run_files` | 평가용 모델 파일과 로컬 run 파일 정리 | — |
| `register_model` | 후보를 등록하고 champion보다 나으면 승격 | (선택) `candidate_task`, `champion_task` |
| `apply_schema` | 자기 폴더의 `storage/schema.sql` 적용 (데이터 소스: 원본·실시간 표, 모델: 예측 기록 표) | — |
| `apply_features` | Feast 정의 파일이 바뀌었을 때만 모두 모아 적용 (데이터 소스용) | — |
| `materialize_features` | 새로 들어온 데이터를 Redis에 반영 (데이터 소스용) | — |
| `check_performance`, `check_drift` | 성능·입력 분포 점검 | — |
| `request_retraining` | 재학습이 필요하고 가능하면 기록하고 다음 작업으로, 아니면 뒤 작업을 건너뜀 | (선택) `pipeline`, `performance_task`, `drift_task` |
| `trigger_pipeline` | 이 모델의 다른 파이프라인 실행 | `pipeline`, (선택) `run_id` |

같은 실행 안의 학습·평가·정리·등록 작업은 실행 시작 시각으로 만든 같은 MLflow run 이름(`train_<날짜>_<시각>`)을 씁니다.
새 operator가 필요하면 운영자에게 요청합니다 (`dags/operators/`의 알맞은 파일에 함수를 만들고 `__init__.py`의 `OPERATORS`에 이름을 올림).

### 7. 학습 돌리기

- Airflow 화면에서 `<모델>_training`을 켜거나 ▶(Trigger)로 실행합니다.
- 정기 학습 주기는 `pipelines/training.yaml`의 `schedule`입니다.
- 실행할 때 `config_path`(학습 설정)와 `data_dir`(학습 데이터 폴더)을 바꿀 수 있습니다. `data_dir`을 다른 폴더로 주면 그 데이터를 그대로 학습합니다.
- 첫 학습은 비교할 champion이 없어서 바로 champion이 됩니다. 그다음부터는 같은 시험 데이터로 평가한 champion보다 나을 때만 승격합니다.
- 결과는 MLflow(실험 = 모델 이름)와 Grafana에서 봅니다. Slurm 로그는 `infra/slurm/logs/model-train-<작업 번호>.out`에 있습니다.

DAG가 꺼진 동안 밀린 정기 실행이 있으면, DAG를 켤 때 그 실행이 바로 시작됩니다. 이때 ▶까지 누르면 학습이 두 번 돕니다.

### 8. 예측·점검 켜기

champion이 생긴 뒤에 켭니다.

1. 서빙 컨테이너를 다시 띄웁니다. 서빙할 모델 목록과 예측 기록 표는 시작할 때만 읽고 만듭니다. champion이 바뀌는 것은 1분 안에 알아서 반영됩니다.
   ```bash
   docker compose -f infra/docker-compose.yml restart serving
   ```
2. Airflow에서 `<모델>_monitoring`을 켭니다.
3. 외부 시스템이 예측 API를 부르면 예측이 저장되고 점검 DAG가 돕니다. 직접 불러 볼 수도 있습니다. 같은 입력이면 저장된 예측을 돌려주고 점검 신호는 보내지 않습니다.
   ```bash
   curl -X POST http://<서버>:8001/models/<모델 이름>/predict
   ```

서빙 앱이 Airflow에 신호를 보낼 계정은 운영자가 처음 한 번 만듭니다 (`.env`의 `AIEMS_AIRFLOW_API_USER`·`AIEMS_AIRFLOW_API_PASSWORD`).

```bash
docker compose -f infra/docker-compose.yml exec airflow bash -c 'airflow users create --role Op --username "$AIEMS_AIRFLOW_API_USER" --password "$AIEMS_AIRFLOW_API_PASSWORD" --firstname serving --lastname aiems --email serving@aiems.local'
```

### 9. 바꾼 뒤 확인하기

`common/`, `dags/`, 기능 폴더의 Airflow용 파일과 `pipelines/` YAML을 고친 뒤에는 꼭 DAG 불러오기 오류를 확인합니다. 오류가 나면 그 파일의 DAG가 모두 사라집니다.

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
