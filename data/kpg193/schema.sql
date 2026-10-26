-- 데이터 소스 kpg193 전용 스키마. 등록 DAG(kpg193_register)가 시작할 때마다 적용합니다. 여러 번 실행해도 안전합니다.
-- 테이블은 data/kpg193/handler/ingest.py가 처음 쓸 때 데이터 열 그대로 만듭니다.
--   원본 (register_dataset, 한 번만): bus, gen, branch, y_bus, runtime, scenarios_agg_load_profile
--   실시간 (register_next_scenario, 15분마다 1개씩): 위 이름 + _realtime, event_timestamp 열이 더해짐

CREATE SCHEMA IF NOT EXISTS kpg193;

-- 인덱스: 원본은 15분마다 시나리오 1개를 꺼낼 때 테이블 전체(2.3GB)를 읽지 않도록,
-- 실시간은 분포 점검·서빙이 최근 시나리오 범위만 읽도록 함.
-- 테이블이 있을 때만 만듦. 테이블이 아직 없으면 건너뛰고,
-- ingest.py가 원본을 다시 등록하거나 실시간 테이블을 처음 만든 뒤 이 파일을 다시 실행함
DO $$
DECLARE
    source record;
BEGIN
    FOR source IN
        SELECT * FROM (VALUES
            ('bus', 'scenario'),
            ('gen', 'scenario'),
            ('branch', 'scenario'),
            ('y_bus', 'scenario'),
            ('runtime', 'scenario'),
            ('scenarios_agg_load_profile', 'load_scenario'),
            ('bus_realtime', 'scenario'),
            ('gen_realtime', 'scenario'),
            ('branch_realtime', 'scenario'),
            ('y_bus_realtime', 'scenario'),
            ('runtime_realtime', 'scenario'),
            ('scenarios_agg_load_profile_realtime', 'scenario')
        ) AS t(table_name, column_name)
    LOOP
        IF to_regclass(format('kpg193.%I', source.table_name)) IS NOT NULL THEN
            EXECUTE format(
                'CREATE INDEX IF NOT EXISTS %I ON kpg193.%I (%I)',
                source.table_name || '_' || source.column_name || '_idx', source.table_name, source.column_name
            );
        END IF;
    END LOOP;
END $$;
