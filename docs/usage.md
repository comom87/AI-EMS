# 활용 방법

본 문서는 초기 설정부터 데이터 소스·모델 추가, 파이프라인 작성, 학습, 서빙, 점검에 이르는 플랫폼 활용 절차를 설명합니다.
전체 흐름은 [architecture.md](architecture.md), 폴더 및 파일 구조는 [structure.md](structure.md)를 참고하시기 바랍니다.

모든 명령은 저장소 루트(`~/projects/ai-ems`)에서 실행합니다.

## 목차

- [1. 초기 설정](#1-초기-설정)
- [2. 서비스 구동 및 접속](#2-서비스-구동-및-접속)
- [3. 이미지 빌드](#3-이미지-빌드)
- [4. 데이터 소스 추가 (운영자)](#4-데이터-소스-추가-운영자)
- [5. 신규 모델 추가 (모델 개발자)](#5-신규-모델-추가-모델-개발자)
- [6. 파이프라인 작성](#6-파이프라인-작성)
- [7. 학습 실행](#7-학습-실행)
- [8. 예측 및 점검 활성화](#8-예측-및-점검-활성화)
- [9. 변경 사항 확인](#9-변경-사항-확인)
- [10. 파이프라인 없이 학습 및 서빙](#10-파이프라인-없이-학습-및-서빙)

## 1. 초기 설정

저장소를 내려받습니다.

```bash
git clone https://github.com/comom87/AI-EMS.git ~/projects/ai-ems
```

운영자로부터 `.env` 파일을 전달받아 저장소 루트에 두고, 본인만 읽을 수 있도록 권한을 설정합니다.

```bash
chmod 600 .env
```

가상환경은 반드시 저장소 내 `.venv`에 생성합니다. Slurm 작업 스크립트가 종료 시 `.venv/bin/python3`로 자원 사용량을 기록하기 때문입니다. 서버의 기본 `python3`는 3.11이므로 3.12를 직접 지정합니다.

```bash
/usr/bin/python3.12 -m venv .venv && .venv/bin/pip install -r requirements.txt
```

## 2. 서비스 구동 및 접속

서비스는 운영자가 구동하며, 모든 사용자가 공유합니다.

```bash
docker compose -f infra/docker-compose.yml up -d
```

`<서버>`는 서버의 내부망 주소입니다. 포트는 `.env`의 `AIEMS_*_PORT` 항목으로 지정하며, 계정은 운영자에게 발급받습니다.

| 서비스 | 주소 | 인증 |
|---|---|---|
| Airflow | `http://<서버>:8082` | 운영자 발급 계정 |
| Grafana (대시보드 `AI-EMS MLOps`) | `http://<서버>:3001` | 운영자 발급 계정 (비밀번호: `.env`의 `AIEMS_GRAFANA_PASSWORD`) |
| MLflow | `http://<서버>:5002` | 없음 |
| 서빙 API | `http://<서버>:8001/docs` | 없음 |
| PostgreSQL | `<서버>:5434` | `.env`의 `AIEMS_DB_USER`, `AIEMS_DB_PASSWORD` |
| Redis | 서버 내부 전용 (`127.0.0.1:6380`) | 없음 |

## 3. 이미지 빌드

서비스 이미지는 Docker Compose로 빌드합니다.

```bash
docker compose -f infra/docker-compose.yml build airflow serving mlflow
```

학습 이미지는 Docker 이미지만 빌드하면 됩니다. 이미지 이름은 기능 폴더 `training/model_train.py`의 `TRAINING_IMAGE`와 일치해야 합니다 (태그는 `latest`).

```bash
docker compose -f infra/docker-compose.yml --profile build build graphkit
```

모델 전용 이미지는 기능 폴더의 `docker/`에서 빌드합니다.

```bash
docker build -t <이미지 이름> sources/<기능>/docker
```

Slurm은 학습 작업을 Apptainer로 실행하므로 Docker 이미지를 SIF(`infra/slurm/sif/<이미지 이름>.sif`)로 변환하여 사용합니다. 이 변환은 학습 작업 시작 시 `infra/slurm/common.sh`가 자동으로 수행합니다.

- SIF를 생성한 Docker 이미지 ID(`<이미지 이름>.sif.id`)와 현재 이미지 ID가 다른 경우에만 변환합니다. 이미지가 동일하면 즉시 학습을 시작합니다.
- 이미지 변경 후 첫 학습은 변환 시간(대형 이미지는 수십 분)만큼 시작이 지연됩니다. 동일 이미지를 사용하는 작업이 동시에 시작되면 한 작업만 변환하고 나머지는 대기합니다.
- 학습 전에 미리 변환하려면 다음 명령을 실행합니다. 홈 디스크 용량이 부족한 경우 `APPTAINER_TMPDIR`, `APPTAINER_CACHEDIR`을 대용량 디스크로 지정합니다.
  ```bash
  apptainer build --force infra/slurm/sif/aiems-graphkit.sif docker-daemon://aiems-graphkit:latest && docker image inspect -f '{{.Id}}' aiems-graphkit:latest > infra/slurm/sif/aiems-graphkit.sif.id
  ```

## 4. 데이터 소스 추가 (운영자)

1. `data/<소스>/` 폴더를 생성하고 `pipelines/register.yaml`, `storage/schema.sql`, `storage/feature_store.py`, `handler/ingest.py`를 작성합니다. 폴더 이름이 곧 소스 이름이며, 영문 소문자·숫자·밑줄만 사용합니다. `kpg193/`을 참고합니다.
   - `register.yaml`의 `dag:` 항목에 설명(`description`)과 담당자(`owner`)를 기재합니다.
   - 등록 단계(`operator: function`)가 호출하는 함수(예: `ingest.register_new_data`)는 `handler/`에 두며, 첫 번째 인자로 `spec`(`{name: <소스>}`)을 받습니다.
   - Feast 객체 이름에는 소스 이름을 접두어로 붙여 다른 소스와 충돌하지 않도록 합니다.
2. 5분 이내에 Airflow가 `<소스>_register` DAG를 생성합니다. Feast 정의는 해당 DAG의 `apply_features` 단계가 처음 실행될 때 적용됩니다.
3. datakit으로 데이터를 생성하는 경우 `data/<소스>/handler/datakit.py`의 `generate_dataset(source, config_path)`를 사용합니다. 결과는 `data/<소스>/files/`에 저장됩니다.

## 5. 신규 모델 추가 (모델 개발자)

1. **기능 폴더 생성**
   ```bash
   cp -r template sources/<기능 이름>
   ```
   기능 폴더 이름이 곧 모델 이름입니다. MLflow 실험·등록 모델, DB 스키마, DAG 이름 접두어, 서빙 경로(`/models/<기능 이름>/predict`)에 공통으로 사용되므로 영문 소문자·숫자·밑줄로 작성합니다. `common`, `public`, 데이터 소스 이름은 사용할 수 없습니다.
2. **공통 값 정의:** 모델별 설정 파일은 별도로 두지 않으며, 모델 전체가 공유하는 값은 기능 폴더 코드의 상수로 정의합니다.
   - `training/dataset_preparation.py`: `DATA_SOURCE`(구독 데이터 소스), `DATA_DIR`(학습 데이터 출력 폴더, `data/<소스>/files/<기능 이름>`), `TRAINING_CONFIG`(기본 학습 설정)
   - `training/model_train.py`: `TRAINING_IMAGE`(학습 이미지)
   - 서빙·분포 점검 코드는 위 상수를 import하여 사용합니다. 학습 파이프라인 YAML의 `params`(`config_path`, `data_dir`) 기본값도 동일하게 지정합니다.
3. **이름 변경:** 파일 내 `your_function`과 `<모델 이름>`을 모두 기능 폴더 이름으로 변경합니다.
4. **파이프라인 설정:** `pipelines/`의 기본 YAML 2종(`training.yaml`, `monitoring.yaml`)에 담당자(`owner`), 승격 지표(`validate_model`의 `metric`), 점검 기준(`check_performance`, `check_drift`, `request_retraining`의 인자)을 지정합니다. 단계, 순서, 실행 주기도 변경할 수 있습니다 ([6. 파이프라인 작성](#6-파이프라인-작성) 참조).
5. **함수 구현:** 각 파일의 `NotImplementedError` 부분을 구현합니다. 기본 파이프라인이 아래 이름과 인자로 호출하므로, YAML을 변경하지 않는 경우 함수 이름과 인자를 유지합니다.

   | 파일 | 구현 대상 | 준수 사항 |
   |---|---|---|
   | `training/dataset_preparation.py` | `export_dataset`, `validate_dataset`, `describe_dataset` | 데이터는 `data/<소스>/files/<모델 이름>/` 하위에 저장합니다. 검증 실패 시 예외를 발생시킵니다 |
   | `training/model_train.py` | `build_train_job`, `build_evaluate_job` | `image`, `command`, `job_key`를 포함한 dict를 반환합니다 |
   | `training/train.py` | `fit_model`, `evaluate_model` (컨테이너 내부 실행) | 학습 run 이름은 `run_name`을 사용하고, 평가 run에는 `validate_model`의 `metric` 지표를 기록합니다 |
   | `serving/predictor.py` | `load_model`, `fetch_latest_scenario`, `predict_scenario`, `calculate_metrics`, `format_predictions`, `PREDICTION_TABLES` | 지표에 `check_performance`의 `metric_column`이 포함되어야 합니다 |
   | `monitoring/input_drift.py` | `measure_recent_drift`, `measure_new_data_drift`, `measure_champion_data_drift` | measures에 `*_psi` 값과 `out_of_range_ratio`를 포함합니다 |
   | `storage/schema.sql`, `storage/feature_store.py` | 예측 기록 테이블, 피처 묶음 | 기본 열(`scenario`, `model_version`, `predicted_at`)은 변경하지 않습니다 |

6. **라이브러리:**
   - 학습: 공용 graphkit 이미지에 없는 라이브러리가 필요하면 `sources/<기능>/docker/`에 Dockerfile을 작성하고 `docker build -t <이미지 이름> sources/<기능>/docker`로 빌드한 후, `training/model_train.py`의 `TRAINING_IMAGE`에 해당 이름을 지정합니다. SIF 변환은 학습 작업이 자동으로 수행합니다 ([3. 이미지 빌드](#3-이미지-빌드) 참조).
   - 서빙: 서빙 이미지는 모든 모델이 공유하므로, 필요한 라이브러리는 운영자에게 `infra/docker/serving/Dockerfile` 반영을 요청합니다.
7. **반영:** 서비스를 재시작할 필요가 없습니다.
   - Airflow: 5분 이내에 `pipelines/`의 YAML별로 DAG가 생성됩니다. 신규 DAG는 비활성 상태로 생성되므로 Airflow 화면에서 활성화합니다. YAML 오류(존재하지 않는 operator·함수·단계 이름)는 불러오기 오류로 표시되므로 [9. 변경 사항 확인](#9-변경-사항-확인) 절차로 확인합니다.
   - Feast: `storage/feature_store.py`는 등록 DAG의 다음 실행 시(5분 이내) 적용됩니다.
   - 서빙: 1분 이내에 신규 기능 폴더를 인식하여 예측 기록 테이블을 생성하고, champion이 등록되면 모델을 불러옵니다.

## 6. 파이프라인 작성

`sources/<기능>/pipelines/<이름>.yaml` 파일 하나가 DAG `<모델>_<이름>` 하나에 해당합니다(데이터 소스는 `data/<소스>/pipelines/` → `<소스>_<이름>`). 기본 YAML을 수정하거나 새 이름의 파일을 추가하면 해당 DAG가 생성됩니다.

```yaml
dag:
  description: 한 줄 설명
  schedule: "@monthly"                       # cron·프리셋, predictions(서빙 앱이 이 모델의 신규 예측을 저장할 때)
  owner: your_github_account                 # 담당자 (Airflow 화면의 Owner 항목)
  retries: 0                                 # 모든 단계의 기본 재시도 횟수 (단계별 retries로 변경 가능)
  params:                                    # Airflow 화면에서 실행 시 변경 가능한 값. 기본값에 {{ spec.name }}(모델 이름) 사용 가능
    data_dir: data/kpg193/files/your_function

tasks:
  <단계 이름>:
    operator: function                       # 아래 operator 중 하나
    args:                                    # operator에 전달할 값. Airflow 템플릿 사용 가능 ({{ params.data_dir }}, {{ ti.xcom_pull(task_ids='이전 단계') }})
      function: training.dataset_preparation.export_dataset
    dependencies: [<이전 단계 이름>]          # 지정한 단계가 완료된 후 실행
    trigger_rule: all_done                   # (선택) 이전 단계 실패 시에도 실행하려면 all_done
    retries: 3                               # (선택)
    outlets: [predictions]                   # (선택) 완료 시 predictions 신호 전송 (모델 파이프라인 전용, schedule: predictions인 파이프라인이 실행됨)
```

| operator | 기능 | 인자 |
|---|---|---|
| `function` | 기능 폴더(데이터 소스는 `handler/`)의 함수를 `(spec, **args)`로 호출합니다. 반환값은 이후 단계에서 `ti.xcom_pull`로 조회합니다 | `function`(예: `training.dataset_preparation.export_dataset`, `ingest.register_new_data`), 그 외 함수 인자 |
| `slurm_train` | 작업 구성 함수로 Slurm 학습을 제출하고, 학습 run에 데이터 설명 태그와 학습 설정을 기록합니다 | `job`, `config_path`, `data_dir`, (선택) `describe` |
| `slurm_evaluate` | 후보(`candidate`) 또는 champion(`champion`)을 동일한 시험 데이터로 Slurm에서 평가합니다 | `job`, `model`, `config_path`, `data_dir` |
| `measure_new_data_drift` | 학습 데이터 중 직전 학습 이후 신규 누적된 데이터를 기존 데이터와 비교하여 결과를 반환합니다 (XCom) | `data_dir`, `psi_threshold` |
| `record_champion_data_drift` | champion과 후보 모델의 학습 데이터 차이를 학습 run에 기록합니다 | `data_dir`, `psi_threshold`, `out_of_range_threshold` |
| `delete_run_files` | 평가용 모델 파일과 로컬 run 파일을 정리합니다 | — |
| `register_model` | 후보 모델을 등록하고, 평가 지표가 champion보다 우수하면 승격합니다 | `metric`(평가 run의 지표 이름), `lower_is_better`, (선택) `candidate_task`, `champion_task` |
| `apply_schema` | 해당 폴더의 `storage/schema.sql`을 적용합니다 (데이터 소스: 원본·실시간 테이블, 모델: 예측 기록 테이블) | — |
| `apply_features` | Feast 정의 파일이 변경된 경우에만 전체를 수집하여 적용합니다 (데이터 소스용) | — |
| `materialize_features` | 신규 데이터를 Redis에 반영합니다 (데이터 소스용) | — |
| `check_performance` | champion의 최근 예측 지표를 오프라인 평가값과 비교합니다 | `metric_column`, `aggregation`(`pooled_rmse`, `mean`), `window`, `tolerance` |
| `check_drift` | 최근 입력을 champion의 학습 데이터와 비교합니다 | `window`, `psi_threshold`, `out_of_range_threshold` |
| `request_retraining` | 재학습이 필요하고 가능한 경우 요청을 기록하고 다음 단계로 진행하며, 그렇지 않으면 이후 단계를 건너뜁니다 | `retrain_on`(재학습 대상 분포 판정), `cooldown_days`, (선택) `pipeline`, `performance_task`, `drift_task` |
| `trigger_pipeline` | 동일 모델의 다른 파이프라인을 실행합니다 | `pipeline`, (선택) `run_id` |

동일 실행 내의 학습·평가·정리·등록 단계는 실행 시작 시각으로 생성한 MLflow run 이름(`train_<날짜>_<시각>`)을 공유합니다.
새로운 operator가 필요한 경우 운영자에게 요청합니다 (`dags/operators/`의 해당 파일에 함수를 추가하고 `__init__.py`의 `OPERATORS`에 등록).

## 7. 학습 실행

- Airflow 화면에서 `<모델>_training`을 활성화하거나 ▶(Trigger)로 실행합니다.
- 정기 학습 주기는 `pipelines/training.yaml`의 `schedule`로 지정합니다.
- 실행 시 `config_path`(학습 설정)와 `data_dir`(학습 데이터 폴더)을 변경할 수 있습니다. `data_dir`에 다른 폴더를 지정하면 해당 데이터로 학습합니다.
- 최초 학습 모델은 비교 대상이 없으므로 바로 champion으로 등록됩니다. 이후에는 동일한 시험 데이터로 평가한 champion보다 우수한 경우에만 승격합니다.
- 결과는 MLflow(실험 이름 = 모델 이름)와 Grafana에서 확인합니다. Slurm 로그는 `infra/slurm/logs/model-train-<작업 번호>.out`에 저장됩니다.

DAG가 비활성 상태인 동안 누락된 정기 실행이 있으면, DAG를 활성화하는 즉시 해당 실행이 시작됩니다. 이때 ▶를 추가로 실행하면 학습이 중복 수행되므로 주의합니다.

## 8. 예측 및 점검 활성화

champion이 등록된 후 활성화합니다.

1. 서빙은 재시작할 필요가 없습니다. 기능 폴더에 `serving/predictor.py`가 생성되면 1분 이내에 서빙 대상에 추가되고(예측 기록 테이블 생성 포함), champion이 등록되거나 변경되면 1분 이내에 반영됩니다. `GET /health`로 상태를 확인합니다.
2. Airflow에서 `<모델>_monitoring`을 활성화합니다.
3. 외부 시스템이 예측 API를 호출하면 예측 결과가 저장되고 점검 DAG가 실행됩니다. 직접 호출하여 확인할 수도 있습니다. 동일한 입력에 대해서는 저장된 예측을 반환하며 점검 신호를 전송하지 않습니다.
   ```bash
   curl -X POST http://<서버>:8001/models/<모델 이름>/predict
   ```

서빙 재시작은 이미 서빙 중인 모델의 서빙 코드(`serving/` 하위 파일)를 수정한 경우에만 필요합니다. 수정 중인 코드가 일부만 반영되는 것을 방지하기 위해 자동으로 다시 불러오지 않습니다.

```bash
docker compose -f infra/docker-compose.yml restart serving
```

서빙 앱이 Airflow에 신호를 전송할 때 사용하는 계정은 운영자가 최초 1회 생성합니다 (`.env`의 `AIEMS_AIRFLOW_API_USER`, `AIEMS_AIRFLOW_API_PASSWORD`).

```bash
docker compose -f infra/docker-compose.yml exec airflow bash -c 'airflow users create --role Op --username "$AIEMS_AIRFLOW_API_USER" --password "$AIEMS_AIRFLOW_API_PASSWORD" --firstname serving --lastname aiems --email serving@aiems.local'
```

## 9. 변경 사항 확인

`common/`, `dags/`, 기능 폴더의 Airflow용 파일, `pipelines/` YAML을 수정한 후에는 반드시 DAG 불러오기 오류를 확인합니다. 오류가 발생하면 모든 DAG가 중단됩니다.

```bash
docker compose -f infra/docker-compose.yml exec airflow airflow dags list-import-errors
```

등록 DAG의 실행을 기다리지 않고 Feast 정의를 즉시 적용하려면 다음 명령을 실행합니다.

```bash
docker compose -f infra/docker-compose.yml exec airflow python3 -c 'from common.storage.feature_store import apply_feature_definitions; apply_feature_definitions()'
```

`db/`의 SQL을 수정한 경우 직접 적용합니다. 모든 SQL은 반복 실행해도 안전하게 작성되어 있습니다. 데이터 소스의 `schema.sql`은 등록 DAG 시작 시, 모델의 `schema.sql`은 서빙 앱이 해당 모델을 처음 불러올 때 자동으로 적용됩니다.

```bash
docker exec -i aiems-postgres-1 sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < db/slurm_jobs.sql
```

## 10. 파이프라인 없이 학습 및 서빙

파이프라인을 구성하기 전에 모델을 먼저 학습하고 서빙하는 절차입니다.

1. **기능 폴더 생성:** template을 복사한 후, 사용하지 않는 `pipelines/`를 삭제합니다. 삭제하지 않으면 구현되지 않은 함수를 호출하는 DAG가 생성됩니다.
   ```bash
   cp -r template sources/<기능 이름> && rm -r sources/<기능 이름>/pipelines
   ```
2. **공통 값 정의:** `training/dataset_preparation.py`의 `DATA_SOURCE`와 `training/model_train.py`의 `TRAINING_IMAGE`를 지정합니다. 서빙 코드는 `DATA_SOURCE`를, 제출 도구는 `TRAINING_IMAGE`를 사용합니다 (`model_train.py`가 없으면 `aiems-graphkit`).
3. **학습 파일 작성:** 학습 파일에는 학습 코드만 작성합니다. 학습 파일은 컨테이너 내부에서 저장소 루트를 작업 폴더로 하여 실행되며, MLflow 주소는 미리 설정되어 있습니다.
   ```python
   # [학습 컨테이너] <기능 이름> 모델 학습
   import mlflow


   def train_model():
       mlflow.set_experiment("<기능 이름>")              # 실험 이름 = 기능 폴더 이름
       with mlflow.start_run(run_name="manual_20261027"):
           # 데이터 조회 → 학습 → 평가
           mlflow.log_metric("<승격·점검 기준 지표 이름>", 12.3)
           mlflow.log_artifacts("tmp/<기능 이름>/model")   # serving/predictor.py의 load_model(model_dir)이 읽을 파일


   if __name__ == "__main__":
       train_model()
   ```
   학습 데이터를 파일로 내보내는 경우 `data/<소스>/files/<기능 이름>/` 하위에 저장합니다.
4. **작업 제출:** 학습 파일 경로를 지정하면 제출 도구가 나머지 항목을 구성하여 Slurm에 제출합니다.
   ```bash
   .venv/bin/python3 -m common.slurm.submit_script sources/<기능 이름>/training/train.py
   ```
   - 파일 경로로 모델을 식별하고, 해당 기능 폴더의 `TRAINING_IMAGE`로 실행합니다.
   - 최초 실행 시에는 충분한 자원(GPU 100%, CPU 8개, 메모리 32GB)을 요청하고, 동일한 파일·인자로 다시 제출하면 직전 사용량을 기준으로 요청합니다.
   - 학습 파일에 전달할 인자는 경로 뒤에 지정합니다 (예: `... train.py --epochs 50`).
   - 제출 후 즉시 종료됩니다. 진행 상황은 `squeue`, 출력은 `infra/slurm/logs/model-train-<작업 번호>.out`, 결과는 MLflow 실험 `<기능 이름>`에서 확인합니다.
   - 코드를 반복 수정하며 실험하는 경우 GPU를 할당받은 대화형 셸에서 직접 실행할 수도 있습니다.
     ```bash
     srun --partition=gpu --gres=mps:50 --cpus-per-task=4 --mem=32G --qos=default --time=02:00:00 --pty bash
     ```
     ```bash
     apptainer shell --nv --bind $PWD:/workspace --pwd /workspace infra/slurm/sif/<이미지 이름>.sif
     ```
5. **등록 및 champion 지정:** MLflow 화면 또는 코드로 해당 run을 등록하고 `champion` 별칭을 지정합니다.
   ```python
   version = mlflow.register_model(f"runs:/{run_id}", "<기능 이름>").version
   mlflow.MlflowClient().set_registered_model_alias("<기능 이름>", "champion", version)
   ```
6. **서빙 코드 구현:** `serving/predictor.py`, `storage/schema.sql`(예측 기록 테이블), 필요 시 `storage/feature_store.py`(서빙용 피처 묶음)를 구현합니다. 서빙은 재시작 없이 1분 이내에 신규 기능 폴더를 인식하여 champion을 불러옵니다. `GET /health`로 상태를 확인한 후 `POST /models/<기능 이름>/predict`로 사용합니다.

새로 학습한 모델로 교체하려면 5번과 같이 champion 별칭을 변경합니다.

이후 파이프라인을 적용하는 경우 `template/pipelines/`를 복사하고 `training/`, `monitoring/`의 함수를 구현합니다. 점검 DAG는 파이프라인을 통해 승격된 champion부터 정상 동작합니다. 수동으로 등록한 버전에는 점검에 필요한 평가·데이터 설명 태그(`validation.*`, 학습 데이터 설명)가 없기 때문입니다. 따라서 학습 DAG를 한 차례 실행한 후 점검 DAG를 활성화합니다.
