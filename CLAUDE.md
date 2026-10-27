# CLAUDE.md

이 저장소에서 코드를 작성하거나 고칠 때 지키는 원칙입니다. 사람과 Claude 모두에게 적용됩니다.
전체 흐름은 `docs/architecture.md`, 폴더·파일별 설명은 `docs/structure.md`, 사용법은 `docs/usage.md`에 있습니다.

## 코드 작성 원칙

1. **로그 미포함:** `logging`, `print` 기반 상태 출력은 넣지 않습니다.
2. **함수명 규칙:** `동사 + 명사` 형태로 짓고, 언더스코어(`_`)로 시작하지 않습니다.
   - 좋은 예: `fetch_current_state`, `generate_dataset`, `validate_dataset`, `train_model`, `evaluate_model`, `compare_metrics`, `register_model`, `load_production_model`, `predict_grid_state`, `calculate_residual`, `calculate_drift`
   - 지양할 예: `_generate_dataset`(밑줄 시작), `dataset_generator`(명사+명사), `check`(명사 없음)
3. **간결성:** 함수 하나는 책임 하나만 갖고, 클래스 계층이나 추상화 레이어를 새로 만들지 않습니다. 예외는 파이프라인 조립(`dags/pipeline_builder.py`, `dags/operators/`) 하나뿐이고, 여기서도 operator는 클래스가 아니라 함수입니다.
4. **파일도 책임 하나:** 파일 하나는 한 가지 일만 합니다. 예를 들어 파이프라인 YAML은 DAG마다, Slurm 기록은 표마다 한 파일입니다. 한 파일에 다른 일이 섞이기 시작하면 나눕니다.
5. **주석과 메시지:** 주석은 한국어로 "왜"와 "언제 불리는지"를 씁니다. 예외 메시지는 영어로 씁니다.
6. **실행 위치 표시:** 기능 폴더(`sources/<기능>/`)의 파이썬 파일은 첫 줄 주석에 실행 위치를 적습니다: `[Airflow]`, `[학습 컨테이너]`, `[서빙]`, `[Feast]`.

## 어디에 무엇을 두나

| 폴더 | 무엇 | 관리 |
|---|---|---|
| `common/` | 모델과 무관한 플랫폼 코드: Airflow와 무관한 일반 함수. `config/`(설정·모델·데이터 소스 찾기·신호 주소), `storage/`(DB 스키마·Feast), `slurm/`(제출·자원·기록), `model_registry/`(MLflow·승격), `monitoring/`(성능·분포 점검, 재학습 판단) | 운영자 |
| `data/<소스>/` | 데이터 하나에 관한 것 전부 (폴더 하나가 데이터 소스 하나, 폴더 이름이 소스 이름): `pipelines/`(등록 파이프라인 YAML, 파일마다 DAG `<소스>_<이름>`), `storage/`(`schema.sql` DB 스키마, `feature_store.py` 피처 뷰), `configs/`, `handler/`(등록·생성 코드), `files/`(실제 데이터 파일) | 운영자 |
| `sources/<기능>/` | 기능 하나와 그 모델 하나에 관한 것 전부: `models/configs/`(학습 설정), `pipelines/`(파이프라인 YAML, 파일마다 DAG `<모델>_<이름>`. 담당자·주기·승격·점검 기준값도 여기), `storage/`(`schema.sql` 예측 기록 표, `feature_store.py` 피처 묶음), `training/`, `serving/`, `monitoring/`, 필요하면 `docker/`(전용 학습 이미지) | 모델 개발자 |
| `template/` | 새 기능 폴더의 복사 원본 (기본 파이프라인 YAML, 함수 빈 틀) | 운영자 |
| `serving/` | 모든 모델이 같이 쓰는 서빙 앱: 외부 시스템이 부를 때 예측하고, 새 예측을 저장하면 Airflow에 신호를 보내 점검 DAG를 실행 | 운영자 |
| `dags/` | Airflow에 관한 것 전부: 진입점 `pipeline_dags.py`(Airflow가 읽는 유일한 파일), YAML 조립 `pipeline_builder.py`, operator `operators/`(`__init__.py`에 이름 목록, 단계 함수는 쓰이는 곳별 파일), DAG 이름 규칙 `dag_settings.py`. 진입점 말고는 `.airflowignore`에 올림 | 운영자 |
| `db/` | 플랫폼 DB SQL (새 DB를 처음 띄울 때 이름순으로 자동 실행, 파일끼리 서로 기대지 않게 씀) | 운영자 |
| `infra/` | compose, 이미지, Slurm 실행 스크립트, Feast 설정, Grafana | 운영자 |

## 기능 폴더 약속

플랫폼(`common/<영역>/pipelines/`), 데이터 소스, 모델의 DAG는 모두 `pipelines/<이름>.yaml`(dag, tasks: operator·args·dependencies)로 조립됩니다. 단계·순서·주기는 YAML에서 자유롭게 바꾸고, 플랫폼 일(Slurm 제출, 등록·승격, 점검)은 `dags/operators/`의 operator를 씁니다. 새 operator는 쓰이는 곳에 맞는 파일(`training.py`, `monitoring.py` 등)에 함수를 만들고 `dags/operators/__init__.py`의 `OPERATORS`에 이름을 올립니다.

YAML이 가리키는 기능 폴더 함수는 operator가 정한 인자로 불립니다. 기본 파이프라인(`template/pipelines/`)과 서빙 앱이 부르는 함수는 아래와 같고, 빈 틀과 자세한 약속은 `template/`의 각 파일 주석에 있습니다.

| 파일 | 함수 | 부르는 곳 |
|---|---|---|
| `training/dataset_preparation.py` | 상수 `DATA_SOURCE`, `DATA_DIR`, `TRAINING_CONFIG`, `export_dataset(spec)`, `validate_dataset(spec, config_path, data_dir)`, `describe_dataset(spec, config_path, data_dir)` | `function`, `slurm_train`(describe) operator |
| `training/model_train.py` | 상수 `TRAINING_IMAGE`, `build_train_job(spec, config_path, data_dir, exp_name, run_name)`, `build_evaluate_job(spec, config_path, data_dir, exp_name, run_name, model_run_id, temp_dir)` | `slurm_train`, `slurm_evaluate` operator (돌려준 dict로 Slurm 제출), 제출 도구(`TRAINING_IMAGE`) |
| `serving/predictor.py` | `load_model`, `fetch_latest_scenario`, `predict_scenario`, `calculate_metrics`, `format_predictions`, `PREDICTION_TABLES` | 서빙 앱 |
| `monitoring/input_drift.py` | `measure_recent_drift(spec, champion_run_id, window, psi_threshold)`, `measure_new_data_drift(spec, data_dir, psi_threshold)`, `measure_champion_data_drift(spec, champion_run_id, data_dir, psi_threshold)` | `check_drift`, `measure_new_data_drift`, `record_champion_data_drift` operator |

- `function` operator로 부르는 함수는 첫 인자로 `spec`(모델은 `{name: <모델>, function: <기능 폴더>, kind: model}`, 데이터 소스는 `{name: <소스>}`)을 받고, 나머지는 YAML의 args 이름 그대로 받습니다. 모델은 기능 폴더에서, 데이터 소스는 `handler/`에서 찾습니다.
- 학습 작업은 MLflow 실험 `<모델 이름>`에 run 이름 `run_name`으로 기록합니다. 평가 작업은 `<run_name>_eval`(후보)과 `<run_name>_champion_eval`(champion)에 학습 파이프라인 `validate_model`(register_model)의 `metric` 지표를 남깁니다. 승격 판단은 이 두 값을 비교합니다.
- 시험 데이터를 고르는 방법은 데이터가 늘어나도 바뀌지 않게 합니다. 그래야 후보와 champion을 같은 시험 데이터로 비교할 수 있습니다.
- `serving/predictor.py`의 `calculate_metrics`는 점검 파이프라인 `check_performance`의 `metric_column`을 돌려줘야 합니다. `input_drift.py`의 measures에는 `*_psi` 값과 `out_of_range_ratio`가 있어야 합니다.

## 지켜야 할 것

- **한 사람이 기능 하나, 곧 `sources/<기능>/` 폴더 하나(모델 하나)를 맡습니다.** 그 모델의 설정, 학습·평가·서빙 코드, 예측 기록 표, 전용 이미지는 모두 이 폴더 안에 둡니다.
- 모델 설정 파일은 따로 없습니다. 모델 전체가 같이 쓰는 값(데이터 소스, 학습 데이터 폴더, 학습 설정, 학습 이미지)은 `training/dataset_preparation.py`와 `training/model_train.py`의 상수에 한 번만 두고 다른 코드는 import해서 씁니다. 단계·주기·승격·점검 기준값은 `pipelines/` YAML에 둡니다.
- 모델은 `DATA_SOURCE`의 데이터를 구독만 합니다. 다른 기능 폴더의 코드는 가져다 쓰지 않습니다. 데이터 쪽 코드(`data/<소스>/`)는 가져다 써도 됩니다.
- 기능 폴더의 `[Airflow]` 파일(`training/dataset_preparation.py`, `training/model_train.py`, `monitoring/input_drift.py`)에서는 torch를 import하지 않습니다. Airflow 이미지에 없기 때문입니다. torch가 필요한 일은 `[학습 컨테이너]` 파일에서 합니다.
- **실제 데이터 파일은 반드시 `data/<소스>/files/` 안에 둡니다.** git은 `files/`만 빼므로, 다른 곳에 만든 데이터 파일은 git에 올라갈 수 있습니다. 모델이 학습용으로 내보내는 데이터는 다른 모델과 겹치지 않게 `files/` 아래 자기 폴더에 둡니다.
- 이름 앞부분으로 주인을 나타냅니다. 소스 이름은 DB 스키마·Feast 이름·등록 DAG 이름의 앞부분이고, 모델 이름은 MLflow 실험·DB 스키마·모델 DAG 이름의 앞부분입니다.
- Airflow `dag_id`에는 밑줄 두 개(`__`)를 쓰지 않습니다. 예: `optimal_power_flow_training`, `kpg193_register`, `platform_slurm_job_records`
- 모든 `schema.sql`과 `db/`의 SQL은 여러 번 실행해도 안전하게 씁니다 (`CREATE ... IF NOT EXISTS`). 데이터 소스의 것은 등록 DAG가 시작할 때마다, 모델의 것은 서빙 앱이 그 모델을 불러올 때마다 적용됩니다.
- `common/`, `dags/`, 기능 폴더의 `[Airflow]` 파일과 `pipelines/` YAML을 고친 뒤에는 `airflow dags list-import-errors`로 오류가 없는지 꼭 확인합니다. 불러오기 오류가 나면 그 파일의 DAG가 모두 멈춥니다. 모든 파이프라인이 `pipeline_dags.py` 한 파일에서 만들어지므로, YAML 하나의 실수가 데이터 등록과 플랫폼 정리를 포함한 모든 DAG를 멈춥니다.
- 구조·흐름·사용법이 바뀌는 코드를 고치면 `docs/`의 해당 문서도 같은 커밋에서 고칩니다. 문서가 코드와 어긋나면 다른 사용자가 옛 방법을 따라 하게 됩니다.
- 비밀번호와 서버별 값은 `.env`에만 둡니다. 코드와 설정 파일에는 `.env`의 항목 이름으로만 참조합니다. `.env`는 git에 올리지 않습니다.
- `../opengridfm-mlops`는 지금 운영 중인 기존 파이프라인입니다. 참고만 하고 수정하지 않습니다. 운영 DB와 MLflow에도 전환 전에는 읽기만 합니다.
