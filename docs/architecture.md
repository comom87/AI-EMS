# 시스템 구성 및 흐름

본 문서는 AI-EMS MLOps 플랫폼의 시스템 구성과, 데이터 등록부터 학습·승격·서빙·점검·재학습에 이르는 전체 흐름을 설명합니다.
폴더 및 파일 구조는 [structure.md](structure.md), 활용 방법은 [usage.md](usage.md)를 참고하시기 바랍니다.

## 목차

- [1. 시스템 개요](#1-시스템-개요)
- [2. 구성 요소](#2-구성-요소)
- [3. YAML 기반 파이프라인 구성](#3-yaml-기반-파이프라인-구성)
- [4. 처리 흐름](#4-처리-흐름)
  - [4-1. 데이터 등록: `kpg193_register` (5분 주기)](#4-1-데이터-등록-kpg193_register-5분-주기)
  - [4-2. 학습: `<모델>_training` (정기 실행, 수동 실행, 재학습 요청)](#4-2-학습-모델_training-정기-실행-수동-실행-재학습-요청)
  - [4-3. 서빙: 외부 호출](#4-3-서빙-외부-호출)
  - [4-4. 점검 및 재학습: `<모델>_monitoring` (신규 예측 시)](#4-4-점검-및-재학습-모델_monitoring-신규-예측-시)
  - [4-5. 분포 비교 유형](#4-5-분포-비교-유형)
- [5. 명명 규칙](#5-명명-규칙)
- [6. 신규 모델의 운영 반영 절차](#6-신규-모델의-운영-반영-절차)

## 1. 시스템 개요

```
                 ┌──────────────────────── 단일 서버 (내부망) ─────────────────────────┐
 외부 시스템 ──HTTP──▶ 서빙 API ──예측 저장──▶ PostgreSQL ◀──등록── Airflow 등록 DAG (5분 주기)
 (응용·EMS)    (serving/)  │                    │   ▲                  │
                  ▲        │ 신규 예측 신호      │   │ 오프라인 저장소     └─▶ Redis (Feast 온라인 저장소)
                  │        ▼                    │   │                          │
                  │   Airflow 점검 DAG ──기준 초과──▶ Airflow 학습 DAG ──sbatch──▶ Slurm + Apptainer (GPU)
                  │        │                                  │                    │
                  │        └──────── 점검 기록 ──────▶ Grafana ◀── 학습·평가 기록 ── MLflow
                  └─────────────── 1분 주기 champion 확인 ─────────────────────────────┘
```

플랫폼은 세 개의 계층으로 구성되며, 계층별로 담당자가 구분됩니다.

| 계층 | 위치 | 담당 | 역할 |
|---|---|---|---|
| 데이터 계층 | `data/<소스>/` | 운영자 | 데이터 정의, 등록, Feast 피처 정의. 모델은 데이터를 구독하여 사용 |
| 모델 계층 | `sources/<기능>/` | 모델 개발자 | 기능 하나당 모델 하나. 학습·평가·서빙·분포 비교 코드와 파이프라인 정의 |
| 플랫폼 계층 | `common/`, `dags/`, `serving/`, `infra/` | 운영자 | 모든 모델이 공유하는 흐름: 학습 작업 제출, 승격, 서빙, 점검, 재학습 |

응용 개발자는 서빙 API(`POST /models/<모델>/predict`)를 통해 예측 결과를 활용합니다.

## 2. 구성 요소

모든 서비스는 단일 서버에서 Docker Compose(`infra/docker-compose.yml`)로 구동하며, 학습 작업은 Slurm을 통해 실행합니다.

| 구성 요소 | 역할 |
|---|---|
| PostgreSQL | 데이터 소스 스키마(원본·실시간 테이블), 모델 스키마(예측 요청·예측값·예측 지표), `common` 스키마(Slurm 작업 이력, 점검 기록, 재학습 요청)를 저장합니다. Feast 오프라인 저장소이자 MLflow·Airflow의 메타데이터 저장소입니다 |
| Redis | Feast 온라인 저장소입니다. 서빙이 최신 입력을 빠르게 조회하는 데 사용하며, 키는 1일 후 만료됩니다 |
| Feast | 피처 정의를 단일 프로젝트(`aiems`)로 관리합니다. 데이터 소스는 피처 뷰를, 모델은 사용하는 피처를 묶은 FeatureService를 정의합니다 |
| Apache Airflow | 모든 파이프라인(DAG)을 실행합니다. DAG는 파이프라인 YAML로 조립됩니다 |
| Slurm, Apptainer | GPU 학습·평가 작업을 스케줄링합니다. MPS를 이용해 GPU 한 장을 여러 작업이 나누어 사용하며, Docker 이미지를 SIF로 변환하여 Apptainer로 실행합니다 |
| MLflow | 실험(모델 이름)별 학습·평가 run, 모델 레지스트리, champion 별칭을 관리합니다 |
| 서빙 앱 (FastAPI) | 단일 프로세스에서 모든 모델의 champion을 제공합니다. 예측 결과를 저장하고 Airflow에 점검 신호를 전송합니다 |
| Grafana | 성능·분포 점검 결과, 학습 이력, Slurm 자원 사용량 대시보드를 제공합니다 |

## 3. YAML 기반 파이프라인 구성

Airflow가 읽는 DAG 파일은 `dags/pipeline_dags.py` 하나입니다. 이 파일은 5분 주기로 다시 읽히며, 그때마다 모든 `pipelines/*.yaml`을 찾아 DAG를 생성합니다.

| YAML 위치 | DAG 이름 | 현재 DAG |
|---|---|---|
| `common/<영역>/pipelines/<이름>.yaml` | `platform_<이름>` | `platform_slurm_job_records` |
| `data/<소스>/pipelines/<이름>.yaml` | `<소스>_<이름>` | `kpg193_register` |
| `sources/<기능>/pipelines/<이름>.yaml` | `<모델>_<이름>` | `optimal_power_flow_training`, `optimal_power_flow_monitoring` |

YAML에는 단계(`tasks`)별로 플랫폼이 제공하는 operator, 인자, 실행 순서를 정의합니다. 모델별로 달라지는 작업(데이터 내보내기, 학습 명령 구성, 분포 비교 대상 선정)은 기능 폴더의 함수가 수행하고, 공통 작업(Slurm 제출, 등록·승격, 점검)은 operator가 수행합니다.
담당자, 실행 주기, 승격 지표, 점검 기준과 같은 설정값도 YAML에 함께 정의합니다. 따라서 모델 개발자는 플랫폼 코드를 수정하지 않고 단계, 순서, 주기, 기준값을 변경하거나 새로운 파이프라인을 추가할 수 있습니다.

모델 전체가 공유하는 값(구독 데이터 소스, 학습 데이터 폴더, 기본 학습 설정, 학습 이미지)은 별도의 설정 파일 없이 기능 폴더 코드의 상수(`training/dataset_preparation.py`, `training/model_train.py`)로 정의합니다.

DAG 간 신호는 하나입니다. 서빙 앱이 신규 예측을 저장하면 `aiems://predictions/<모델>` 신호를 전송하고, `schedule: predictions`로 지정된 파이프라인(점검)이 실행됩니다.

## 4. 처리 흐름

### 4-1. 데이터 등록: `kpg193_register` (5분 주기)

```
apply_schema → apply_features → register_data → materialize_features
```

1. **apply_schema:** `data/kpg193/storage/schema.sql`을 적용합니다. 반복 실행해도 안전하게 작성되어 있습니다.
2. **apply_features:** 모든 데이터 소스와 기능 폴더의 `storage/feature_store.py`를 수집하여 Feast에 적용합니다. 정의 파일의 해시가 변경된 경우에만 적용합니다.
3. **register_data:** 원본 시나리오(9,994개) 중 다음 순번의 시나리오 하나를 실시간 테이블(`*_realtime`)에 등록합니다. 실제 계통 데이터가 지속적으로 유입되는 상황을 모사합니다.
4. **materialize_features:** 신규 데이터를 Redis에 반영합니다. 서빙은 이 데이터를 최신 입력으로 사용합니다.

### 4-2. 학습: `<모델>_training` (정기 실행, 수동 실행, 재학습 요청)

```
export_dataset → validate_dataset → measure_new_data_drift → train_model ─┬→ evaluate_model → evaluate_champion ─┬→ delete_run_files → validate_model
                                                                           └→ record_champion_data_drift ──────────┘
```

| 단계 | 내용 | 실행 위치 |
|---|---|---|
| `export_dataset` | 누적된 실시간 시나리오 전체를 학습 형식(parquet)으로 `data/<소스>/files/` 하위에 내보냅니다 | Airflow |
| `validate_dataset` | 내보낸 데이터를 검증합니다. 검증에 실패하면 학습을 진행하지 않습니다 | Airflow |
| `measure_new_data_drift` | 직전 학습 이후 신규로 누적된 데이터를 기존 데이터와 비교합니다. 결과는 XCom에 기록되며 학습 진행에는 영향을 주지 않습니다 | Airflow |
| `train_model` | 기능 폴더에서 구성한 학습 작업을 Slurm에 제출하고 완료를 대기합니다. 학습 run에 데이터 설명 태그와 학습 설정을 기록합니다 | Slurm (GPU) |
| `evaluate_model` | 후보 모델을 시험 데이터로 평가합니다 (run `<run>_eval`) | Slurm (GPU) |
| `evaluate_champion` | 현재 champion을 **동일한 시험 데이터**로 재평가합니다 (run `<run>_champion_eval`) | Slurm (GPU) |
| `record_champion_data_drift` | champion과 후보 모델의 학습 데이터 차이를 학습 run에 기록합니다 | Airflow |
| `delete_run_files` | 평가를 위해 내려받은 모델 파일(`tmp/`)과 로컬 run 파일(`mlruns/`)을 정리합니다 | Airflow |
| `validate_model` | 후보 모델을 MLflow에 등록하고, YAML에 지정한 `metric`이 champion보다 우수한 경우에만 champion으로 승격합니다. 최초 학습 모델은 바로 champion이 됩니다. 지표 이름과 방향은 모델 버전 태그로 기록되어 점검 단계에서 사용됩니다 | Airflow |

- 동일 실행의 모든 단계는 실행 시작 시각으로 생성한 MLflow run 이름(`train_<날짜>_<시각>`)을 공유합니다.
- 시험 데이터 선정 방식은 데이터가 증가해도 변하지 않도록 설계합니다. 예를 들어 `optimal_power_flow`는 부하 시나리오 번호를 10개 구간으로 나누어 9번 구간을 시험 데이터로 사용합니다. 이를 통해 후보 모델과 champion을 공정하게 비교할 수 있습니다.

**Slurm 학습 작업의 처리 과정**

```
sbatch (common/slurm/scheduling.py)
 └ Slurm 작업 (GPU MPS 비율, CPU, 메모리 할당)
    ├ Docker 이미지가 SIF 생성 시점과 다르면 SIF를 재생성 (infra/slurm/common.sh)
    ├ apptainer run --nv <이미지>.sif  (저장소를 /workspace로 연결)
    │   └ 기능 폴더의 학습 명령 (예: 데이터 분할 → graphkit 학습 → 결과를 MLflow에 업로드)
    └ 5초 주기로 GPU·CPU·메모리 사용량을 측정하여 common.slurm_job_usage에 기록
```

- **자원 산정:** 동일 작업의 과거 최대 사용량에 1.3배를 적용하여 GPU(MPS 비율), CPU, 메모리, GPU 메모리를 요청하고, 요청 비율에 따라 QOS(small, default, large)를 선택합니다. 최초 실행 시에는 충분한 자원을 요청합니다.
- **메모리 부족 처리:** 메모리 부족으로 실패한 경우 다음 요청량을 상향 기록하며, 학습 단계는 최대 3회 재시도합니다.
- **이력 보완:** 작업 종료 시 기록되지 않은 이력은 `platform_slurm_job_records`가 15분 주기로 `sacct`를 조회하여 보완합니다.

### 4-3. 서빙: 외부 호출

```
외부 시스템 → POST /models/<모델>/predict
 ├ 최신 시나리오 번호와 입력을 Redis(Feast)에서 조회
 ├ champion 모델로 예측 → 예측값·지표를 모델 스키마에 저장
 └ 신규 예측인 경우 Airflow REST API로 aiems://predictions/<모델> 신호 전송 → 점검 DAG
```

- **중복 계산 방지:** 동일한 시나리오와 모델 버전에 대한 요청은 저장된 예측을 반환하며, 점검 신호도 전송하지 않습니다.
- **변경 사항 자동 반영:** 1분 주기로 다음 사항을 확인합니다.
  - 신규 기능 폴더(`serving/predictor.py`가 있는 폴더)가 있으면 예측 기록 테이블을 생성하고 서빙 대상에 추가합니다.
  - champion 버전이 변경되었으면 새 모델을 내려받아 교체합니다.
  - 따라서 서빙 재시작은 이미 서빙 중인 모델의 서빙 코드를 수정한 경우에만 필요합니다.
- **장애 격리:** 특정 모델을 불러오지 못하더라도 다른 모델은 계속 서빙합니다. 불러오기 실패(`load_errors`), champion 교체 실패(`refresh_error`), 신호 전송 실패(`signal_error`)는 `/health`에서 확인할 수 있습니다.

### 4-4. 점검 및 재학습: `<모델>_monitoring` (신규 예측 시)

```
check_performance ─┬→ request_retraining → trigger_training
check_drift ───────┘
```

| 단계 | 내용 |
|---|---|
| `check_performance` | champion의 최근 `window`건(288건, 5분 주기 기준 1일) 예측 지표를 집계하여, 승격 당시 오프라인 평가값보다 `tolerance`(20%)를 초과하여 저하되면 성능 저하로 판정합니다. 결과는 `common.model_performance_checks`에 기록합니다 |
| `check_drift` | 최근 `window`건(288건)의 입력을 champion의 학습 데이터와 비교(PSI, 학습 범위 이탈 비율)하여 stable, shifted, out_of_range로 판정하고 `common.model_drift_checks`에 기록합니다 |
| `request_retraining` | 성능 저하 또는 `retrain_on`(out_of_range)에 해당하는 판정이 있으면 재학습을 요청합니다. 단, 학습이 진행 중이거나 최근 `cooldown_days`(7일) 이내에 학습한 경우에는 요청하지 않습니다. 요청은 `common.model_retraining_requests`에 기록합니다 |
| `trigger_training` | 학습 DAG를 `retrain_<시각>` 실행으로 시작합니다 (4-2 참조) |

기준값(`window`, `tolerance`, 임계값, `retrain_on`, `cooldown_days`)은 모두 점검 파이프라인 YAML의 단계별 인자로 정의합니다. 기록이 `window`건 이상 누적되기 전에는 판정하지 않습니다.

### 4-5. 분포 비교 유형

동일한 비교 방식(PSI, 학습 범위 이탈 비율)을 세 단계에서 서로 다른 대상에 적용합니다. 비교 대상의 세부 항목은 기능 폴더의 `monitoring/input_drift.py`에서 정의합니다.

| 단계 | 기준 데이터 | 비교 대상 | 결과 저장 | 용도 |
|---|---|---|---|---|
| 학습 DAG `measure_new_data_drift` | 직전 학습까지의 데이터 | 이후 신규 누적 데이터 | XCom | 참고 |
| 학습 DAG `record_champion_data_drift` | champion의 학습 데이터 | 후보 모델의 학습 데이터 | 학습 run의 `drift/*`, `aiems.drift_status` | 참고 |
| 점검 DAG `check_drift` | champion의 학습 데이터 | 최근 실시간 입력 288건 | `common.model_drift_checks` | 재학습 판단 |

승격 여부는 분포가 아닌 평가 지표만으로 결정합니다.

## 5. 명명 규칙

이름의 앞부분으로 자원의 소유 주체를 구분합니다.

| 이름 | 사용처 |
|---|---|
| 데이터 소스 이름 (`data/` 하위 폴더 이름, 예: `kpg193`) | DB 스키마, Feast 객체 이름 접두어, 등록 DAG 이름 접두어 |
| 모델 이름 (`sources/` 하위 기능 폴더 이름, 예: `optimal_power_flow`) | MLflow 실험·등록 모델, DB 스키마, DAG 이름 접두어, 서빙 경로 `/models/<모델>/predict`, 학습 데이터 폴더 |
| `platform` | 플랫폼 DAG 이름 접두어 |
| `common` | 플랫폼 DB 스키마 (Slurm 작업 이력, 점검 기록) |

## 6. 신규 모델의 운영 반영 절차

```
기능 폴더 생성 (template 복사) ─▶ 5분 이내 DAG 생성 (비활성 상태) ─▶ 학습 DAG 활성화·실행 ─▶ 최초 champion 등록
      ─▶ 1분 이내 서빙 시작 ─▶ 점검 DAG 활성화 ─▶ 외부 요청마다 예측·점검 ─▶ 기준 초과 시 자동 재학습
```

이 과정에서 서비스 재시작은 필요하지 않습니다. 세부 절차는 [usage.md](usage.md#5-신규-모델-추가-모델-개발자)를 참고하시기 바랍니다.
