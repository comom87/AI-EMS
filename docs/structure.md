# 폴더 및 파일 구조

본 문서는 저장소의 폴더 구성과 파일별 역할을 설명합니다.
전체 흐름은 [architecture.md](architecture.md), 활용 방법은 [usage.md](usage.md)를 참고하시기 바랍니다.

## 목차

- [1. 최상위 구성](#1-최상위-구성)
- [2. `common/` — 플랫폼 공용 코드](#2-common--플랫폼-공용-코드)
- [3. `data/<소스>/` — 데이터 소스 (예: `kpg193/`)](#3-data소스--데이터-소스-예-kpg193)
- [4. `sources/<기능>/` — 기능 및 모델 (예: `optimal_power_flow/`)](#4-sources기능--기능-및-모델-예-optimal_power_flow)
- [5. `template/` — 신규 기능 폴더 원본](#5-template--신규-기능-폴더-원본)
- [6. `serving/` — 공용 서빙 앱](#6-serving--공용-서빙-앱)
- [7. `dags/` — Airflow 관련 코드](#7-dags--airflow-관련-코드)
- [8. `db/` — 플랫폼 데이터베이스 SQL](#8-db--플랫폼-데이터베이스-sql)
- [9. `infra/` — 서비스 및 서버 설정](#9-infra--서비스-및-서버-설정)

## 1. 최상위 구성

| 파일·폴더 | 설명 |
|---|---|
| `README.md` | 프로젝트 개요 및 문서 안내 |
| `CLAUDE.md` | 코드 작성 원칙 및 준수 사항 (개발자와 Claude 공통 적용) |
| `docs/` | 문서: 시스템 구성 및 흐름(`architecture.md`), 폴더 및 파일 구조(`structure.md`, 본 문서), 활용 방법(`usage.md`) |
| `.env` | 환경 설정 및 인증 정보. git에서 관리하지 않으며 운영자가 배포합니다 |
| `requirements.txt` | 호스트 가상환경(`.venv`)의 파이썬 패키지 목록 |
| `common/` | 모델과 무관한 플랫폼 공용 코드 |
| `data/` | 데이터 소스별 폴더 |
| `sources/` | 기능별 폴더 (기능 하나당 모델 하나) |
| `template/` | 신규 기능 폴더의 원본 |
| `serving/` | 모든 모델이 공유하는 서빙 앱 |
| `dags/` | Airflow 관련 코드: 진입점, 파이프라인 YAML 조립, operator |
| `db/` | 플랫폼 데이터베이스 SQL |
| `infra/` | Docker, Slurm, Feast, Grafana 설정 |

## 2. `common/` — 플랫폼 공용 코드

Airflow에 의존하지 않는 일반 함수로 구성됩니다. Airflow와 연결하는 코드는 `dags/`에 위치합니다.

| 파일 | 역할 |
|---|---|
| **config/** | |
| `settings.py` | 저장소 경로, `.env` 설정값 조회(`read_setting`), 데이터베이스·MLflow 주소 |
| `source_specs.py` | 데이터 소스(`data/` 하위 폴더, 폴더 이름이 곧 소스 이름)를 조회하고 소스의 `handler/` 코드를 불러옵니다 |
| `model_specs.py` | 모델(`sources/` 하위에서 이름 규칙을 만족하는 폴더)을 조회하고 이름을 검증하며, 기능 폴더 코드를 불러옵니다 |
| `signals.py` | DAG 간 신호 주소(`aiems://predictions/<모델>`). Airflow(`dags/pipeline_builder.py`)와 서빙 앱(`serving/signals.py`)이 공유합니다 |
| **storage/** | |
| `schemas.py` | 데이터 소스·모델의 `schema.sql`을 데이터베이스에 적용합니다 |
| `feature_store.py` | 분산된 Feast 정의 파일을 수집하여 적용하고, 데이터 소스별로 Redis에 반영합니다 |
| **slurm/** | |
| `scheduling.py` | 작업 제출(`submit_training_job`) 및 완료 대기. 메모리 부족으로 실패하면 다음 요청량을 상향합니다 |
| `submit_script.py` | 제출 도구. 파이프라인 없이 기능 폴더의 학습 파일 하나를 Slurm 작업으로 제출합니다 (`.venv/bin/python3 -m common.slurm.submit_script <파일> [인자]`). 이미지는 해당 기능 폴더 `training/model_train.py`의 `TRAINING_IMAGE`(없으면 `aiems-graphkit`)를 사용합니다 |
| `sbatch_script.py` | Slurm 작업 스크립트 생성: 컨테이너 실행, 5초 주기 사용량 측정, 종료 후 이력 기록 |
| `resources.py` | 과거 사용량의 1.3배로 GPU(MPS)·CPU·메모리 요청량과 QOS를 산정합니다 |
| `job_records.py` | `common.slurm_jobs`: 작업 단위의 제출 정보, 할당량, 종료 상태, 메모리 최고점 |
| `job_usage.py` | `common.slurm_job_usage`: 5초 간격 사용량 기록 및 p95 요약 |
| `job_history.py` | `common.slurm_job_history`: 동일 작업의 누적 최대 사용량 (다음 요청량 산정 기준) |
| `pipelines/slurm_job_records.yaml` | 플랫폼 파이프라인(DAG `platform_slurm_job_records`). 15분 주기로 종료 이력이 누락된 Slurm 작업의 상태를 `sacct`로 보완합니다 |
| **model_registry/** | |
| `tracking.py` | MLflow run 조회, 태그 기록, 파일 업로드·다운로드 |
| `local_artifacts.py` | 학습 중 로컬에 임시 보관하는 run 파일(`mlruns/`, `tmp/`)의 위치 지정 및 정리 |
| `registry.py` | champion 조회, 후보 모델 등록 및 승격 |
| **monitoring/** | |
| `performance.py` | 최근 예측 지표를 오프라인 평가값과 비교하여 성능 저하를 판정하고 기록합니다 |
| `drift.py` | PSI 계산, 분포 판정(stable, shifted, out_of_range) 및 기록. 비교 대상은 기능 폴더에서 정의합니다 |
| `retraining.py` | 재학습 요청 기록. 재학습 가능 여부는 `dags/operators/monitoring.py`가 Airflow 실행 이력으로 확인합니다 |

## 3. `data/<소스>/` — 데이터 소스 (예: `kpg193/`)

| 파일 | 역할 |
|---|---|
| `pipelines/register.yaml` | 등록 파이프라인(DAG `<소스>_register`). 스키마 적용 → Feast 정의 적용(변경 시) → 데이터 등록 → Redis 반영 순으로 실행하며, 주기와 재시도 횟수를 정의합니다 |
| `storage/schema.sql` | 소스 이름과 동일한 DB 스키마 및 인덱스. 등록 DAG가 시작될 때마다 적용합니다 |
| `storage/feature_store.py` | 데이터 소스가 제공하는 Feast 엔티티와 피처 뷰 (이름에 소스 이름 접두어 사용, 예: `kpg193_bus`) |
| `configs/` | datakit 설정 및 계통 파일 |
| `handler/ingest.py` | 등록 코드: `register_new_data(spec)`(등록 파이프라인의 `function` 단계에서 호출), `register_dataset(source, data_dir)`(원본 일괄 등록 시 직접 실행) |
| `handler/datakit.py` | datakit 컨테이너를 이용한 데이터 생성·검증·통계 |
| `files/` | 실제 데이터 파일 (datakit 결과, 모델이 학습용으로 내보낸 데이터). git에서 제외됩니다 |

## 4. `sources/<기능>/` — 기능 및 모델 (예: `optimal_power_flow/`)

기능 폴더 하나는 모델 하나에 해당하며, 폴더 이름이 곧 모델 이름입니다. `optimal_power_flow/`는 최적 조류 예측 모델입니다.

| 파일 | 실행 위치 | 역할 |
|---|---|---|
| `models/configs/` | — | 학습 설정 |
| `pipelines/*.yaml` | — | 파이프라인 정의: 담당자, 실행 주기, 단계, 승격·점검 기준값. 파일 하나가 DAG 하나에 해당합니다 (`training.yaml` → `<모델>_training`, `monitoring.yaml` → `<모델>_monitoring`) |
| `storage/schema.sql` | — | 예측 기록 테이블(`prediction_requests`, `prediction_metrics`, 예측값 테이블). 서빙 앱이 해당 모델을 처음 불러올 때 적용합니다 |
| `storage/feature_store.py` | Feast | 서빙 시 조회하는 피처 묶음(FeatureService) |
| `training/dataset_preparation.py` | Airflow | 학습 데이터 수집·전처리·검증·설명. 모델 공통 상수 `DATA_SOURCE`(구독 데이터 소스), `DATA_DIR`(학습 데이터 폴더), `TRAINING_CONFIG`(기본 학습 설정)를 정의합니다 |
| `training/model_train.py` | Airflow | 학습·평가 Slurm 작업 구성. 상수 `TRAINING_IMAGE`(학습 이미지)를 정의합니다 |
| `training/split_scenarios.py` | 학습 컨테이너 | 학습·검증·시험 데이터 분할 (graphkit용 torch 파일 생성) |
| `training/publish_results.py` | 학습 컨테이너 | graphkit이 로컬에 기록한 지표와 파일을 MLflow에 업로드합니다 |
| `serving/predictor.py` | 서빙 | 모델 로드, 예측, 지표 계산, 응답 형식 구성 |
| `serving/serving_features.py` | 서빙 | Feast 온라인 저장소(Redis)에서 최신 입력을 조회합니다 |
| `monitoring/input_drift.py` | Airflow | 분포 비교 대상 정의 (총 부하, 정지 설비 수, 모선별 부하) |
| `docker/` | — | (선택) 추가 라이브러리가 필요한 모델의 전용 학습 이미지 |

## 5. `template/` — 신규 기능 폴더 원본

기본 파이프라인 YAML(`pipelines/`, 기준값 설명 포함), 파이프라인이 호출하는 함수의 기본 틀(`training/`, `serving/`, `monitoring/`), 저장 정의 틀(`storage/schema.sql`, `storage/feature_store.py`)로 구성됩니다.
각 함수의 주석에 호출 시점과 반환 형식이 명시되어 있습니다. 사용 방법은 [usage.md의 신규 모델 추가](usage.md#5-신규-모델-추가-모델-개발자)를 참고하시기 바랍니다.

## 6. `serving/` — 공용 서빙 앱

| 파일 | 역할 |
|---|---|
| `app.py` | FastAPI 앱(`POST /models/<모델>/predict`, `GET /models/<모델>`, `GET /models`, `GET /health`). 외부 요청 시 예측을 수행하고, 신규 예측인 경우 점검 신호를 전송합니다 |
| `models.py` | `serving/predictor.py`가 있는 모델을 불러옵니다. 1분 주기로 신규 기능 폴더를 추가(예측 기록 테이블 생성 포함)하고, champion이 변경되면 교체합니다. 불러오지 못한 모델은 `/health`의 `load_errors`에 표시됩니다 |
| `predictions.py` | 예측 결과와 요청 이력을 모델 스키마에 저장하고 조회합니다 |
| `signals.py` | 신규 예측 저장 시 Airflow REST API로 `aiems://predictions/<모델>` 신호를 전송합니다 (계정: `.env`의 `AIEMS_AIRFLOW_API_USER`, `AIEMS_AIRFLOW_API_PASSWORD`). 전송 실패는 `/health`의 `signal_error`에 표시됩니다 |

## 7. `dags/` — Airflow 관련 코드

모든 DAG는 파이프라인 YAML로 조립되며, Airflow가 읽는 파일은 진입점 `pipeline_dags.py` 하나입니다. 그 외 파일과 `operators/` 폴더는 `.airflowignore`에 등록하여 DAG 파일로 읽지 않도록 합니다.
YAML 하나에 오류가 있으면 모든 DAG가 함께 중단되므로, 수정 후에는 반드시 [usage.md의 변경 사항 확인](usage.md#9-변경-사항-확인) 절차를 수행합니다.

| 파일 | 역할 |
|---|---|
| `pipeline_dags.py` | 진입점. 플랫폼·데이터 소스·모델의 `pipelines/*.yaml`을 모두 찾아 DAG를 생성합니다 |
| `pipeline_builder.py` | YAML을 읽고 검증하여 DAG를 조립합니다. `schedule`과 `outlets`의 `predictions`를 DAG 간 신호(Airflow Dataset, 주소는 `common/config/signals.py`)로 변환합니다 |
| `operators/` | YAML에서 사용하는 operator(단계). `__init__.py`에 이름과 함수의 대응 목록이 있고, 단계 함수는 용도별 파일에 있습니다: `general.py`(function, apply_schema), `features.py`(데이터 소스의 Feast), `training.py`(학습·평가·분포 기록·등록), `monitoring.py`(점검·재학습 요청), `slurm_records.py`(플랫폼) |
| `dag_settings.py` | DAG 공통 시작일 및 DAG 명명 규칙(`<소유자>_<파일 이름>`) |
| `.airflowignore` | Airflow가 DAG 파일로 읽지 않을 파일 목록 |

| YAML 위치 | 생성되는 DAG | 예 |
|---|---|---|
| `common/<영역>/pipelines/<이름>.yaml` | `platform_<이름>` | `platform_slurm_job_records` |
| `data/<소스>/pipelines/<이름>.yaml` | `<소스>_<이름>` | `kpg193_register` |
| `sources/<기능>/pipelines/<이름>.yaml` | `<모델>_<이름>` | `optimal_power_flow_training`, `optimal_power_flow_monitoring` |

## 8. `db/` — 플랫폼 데이터베이스 SQL

신규 데이터베이스를 처음 구동할 때 파일 이름 순서로 자동 실행됩니다. 데이터 소스와 모델의 SQL은 각 폴더의 `storage/schema.sql`에 둡니다.

| 파일 | 내용 |
|---|---|
| `service.sql` | MLflow·Airflow용 데이터베이스 |
| `slurm_jobs.sql` | `common` 스키마의 Slurm 작업 이력 테이블 3종 |
| `model_monitoring.sql` | `common` 스키마의 성능 점검·분포 점검·재학습 요청 테이블 |

데이터베이스 스키마 구성은 다음과 같습니다.

| 스키마 | 내용 |
|---|---|
| `common` | Slurm 작업 이력, 모델 점검 기록 |
| `kpg193` | 원본 시나리오(9,994개)와 실시간 데이터(`*_realtime`, 등록 DAG가 순차 적재) |
| `optimal_power_flow` | 모델 `optimal_power_flow`의 예측 요청, 예측값, 예측 지표 |

## 9. `infra/` — 서비스 및 서버 설정

| 파일·폴더 | 설명 |
|---|---|
| `docker-compose.yml` | 서비스: `postgres`, `redis`, `mlflow`, `airflow`, `serving`, `grafana`. 도구: `graphkit`, `datakit` |
| `.env` | 루트 `.env`에 대한 링크 (Docker Compose가 동일한 설정을 사용) |
| `docker/` | 공용 이미지: `graphkit`(학습, MLflow 로거 패치 포함), `serving`, `airflow`, `mlflow`, `datakit` |
| `slurm/common.sh` | 작업 스크립트에서 사용하는 컨테이너 실행 함수(Apptainer SIF). Docker 이미지가 변경된 경우 작업 시작 전에 SIF를 재생성합니다 |
| `slurm/etc/` | 서버 Slurm·MPS 설정 사본 (참고용) |
| `slurm/sif/` | 학습용 SIF 이미지와 해당 SIF를 생성한 Docker 이미지 ID(`.sif.id`). git에서 제외됩니다 |
| `slurm/logs/` | Slurm 작업 로그. git에서 제외됩니다 |
| `feast/feature_store.yaml` | Feast 프로젝트 `aiems` 설정 (Redis 키는 1일 후 만료) |
| `grafana/` | 데이터 소스 설정과 대시보드 JSON(`dashboards/aiems.json`). 대시보드는 화면이 아닌 이 파일을 수정하여 변경합니다 |
