"""
TimescaleDB 테이블 생성 모듈
============================

최초 1회 실행 시 테이블과 인덱스를 생성함.
이미 테이블이 있으면 에러 없이 건너뜀 (IF NOT EXISTS).
"""

from loguru import logger
from .connection import get_connection  # 같은 패키지(db/)의 connection 모듈 사용


# ─────────────────────────────────────────────────────
# 테이블 생성 SQL 문 (설명 포함)
# ─────────────────────────────────────────────────────

# TimescaleDB 확장을 PostgreSQL에 설치하는 SQL
# TimescaleDB는 PostgreSQL 위에 올라가는 시계열 최적화 확장
SQL_CREATE_EXTENSION = """
CREATE EXTENSION IF NOT EXISTS timescaledb;
"""

# env_ecmwf_reanalysis 테이블 생성 SQL
# IF NOT EXISTS → 이미 있으면 에러 없이 건너뜀
SQL_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS env_ecmwf_reanalysis (
    datetime  TIMESTAMPTZ NOT NULL,  -- 관측 시각 (UTC 타임존 포함)
    lat       REAL        NOT NULL,  -- 위도 (-90 ~ 90)
    lon       REAL        NOT NULL,  -- 경도 (-180 ~ 180)

    -- 파랑(Wave) 변수
    swh   REAL,   -- 복합 유의파고: 풍파+너울 합산 (m)
    mwd   REAL,   -- 평균 파랑 진행 방향 (°, 0=북쪽, 시계방향)
    mwp   REAL,   -- 평균 파랑 주기 (초)
    shts  REAL,   -- 너울(swell) 유의파고 (m)
    mdts  REAL,   -- 너울 진행 방향 (°)
    mpts  REAL,   -- 너울 주기 (초)
    shww  REAL,   -- 풍파(wind wave) 유의파고 (m)
    mdww  REAL,   -- 풍파 진행 방향 (°)
    mpww  REAL,   -- 풍파 주기 (초)

    -- 바람(Wind) 변수
    u10   REAL,   -- 10m 고도 동서 방향 풍속 (m/s, 동쪽 방향이 +)
    v10   REAL,   -- 10m 고도 남북 방향 풍속 (m/s, 북쪽 방향이 +)

    -- 복합 기본키: 동일 시각+위치 데이터 중복 적재 방지
    PRIMARY KEY (datetime, lat, lon)
);
"""

# TimescaleDB Hypertable 변환 SQL
# 일반 테이블 → 시계열 최적화 테이블로 변환
# datetime 컬럼 기준으로 7일씩 청크(chunk)로 나눠서 저장
SQL_CREATE_HYPERTABLE = """
SELECT create_hypertable(
    'env_ecmwf_reanalysis',  -- 변환할 테이블 이름
    'datetime',               -- 시간 기준 컬럼
    chunk_time_interval => INTERVAL '7 days',  -- 7일 단위로 청크 분할
    if_not_exists => TRUE     -- 이미 hypertable이면 에러 없이 건너뜀
);
"""

# 위치 기반 조회 속도를 높이기 위한 인덱스
# 특정 위경도 범위의 데이터를 자주 조회하므로 필요
SQL_CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_ecmwf_reanalysis_latlon
    ON env_ecmwf_reanalysis (lat, lon);
"""

# 압축 활성화: 먼저 테이블에 columnstore 압축 기능을 켜야 함
# timescaledb.compress_segmentby: lat,lon 기준으로 묶어서 압축 (위치별 조회 성능 유지)
# timescaledb.compress_orderby: datetime 기준으로 정렬 후 압축 (시간 범위 조회 최적화)
SQL_ENABLE_ECMWF_COMPRESSION = """
ALTER TABLE env_ecmwf_reanalysis SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'lat,lon',
    timescaledb.compress_orderby   = 'datetime DESC'
);
"""

# 30일 이상 된 청크를 자동으로 압축하는 정책 설정
# 압축하면 용량이 50~80% 줄어들지만 쓰기는 불가 (읽기는 가능)
SQL_COMPRESSION_POLICY = """
SELECT add_compression_policy(
    'env_ecmwf_reanalysis',
    INTERVAL '30 days',
    if_not_exists => TRUE
);
"""

# ─────────────────────────────────────────────────────
# HYCOM 해류 데이터 테이블
# ─────────────────────────────────────────────────────

# HYCOM 해류 테이블 생성 SQL
SQL_CREATE_HYCOM_TABLE = """
CREATE TABLE IF NOT EXISTS env_hycom_current (
    datetime  TIMESTAMPTZ NOT NULL,  -- 관측 시각 (UTC, 3시간 간격)
    lat       REAL        NOT NULL,  -- 위도 (-80.48 ~ 80.48)
    lon       REAL        NOT NULL,  -- 경도 (-180 ~ 180, ECMWF와 동일 변환 적용)

    -- 해류(Current) 변수 (수심 0m, 해수면)
    water_u   REAL,   -- 동서 방향 해류 속도 (m/s, 동쪽 방향이 +)
    water_v   REAL,   -- 남북 방향 해류 속도 (m/s, 북쪽 방향이 +)

    PRIMARY KEY (datetime, lat, lon)  -- 중복 적재 방지
);
"""

# HYCOM Hypertable 변환 SQL
SQL_CREATE_HYCOM_HYPERTABLE = """
SELECT create_hypertable(
    'env_hycom_current',
    'datetime',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);
"""

# HYCOM 압축 활성화
SQL_ENABLE_HYCOM_COMPRESSION = """
ALTER TABLE env_hycom_current SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'lat,lon',
    timescaledb.compress_orderby   = 'datetime DESC'
);
"""

# HYCOM 자동 압축 정책
SQL_HYCOM_COMPRESSION_POLICY = """
SELECT add_compression_policy(
    'env_hycom_current',
    INTERVAL '30 days',
    if_not_exists => TRUE
);
"""


# ─────────────────────────────────────────────────────
# ECMWF 예보 데이터 테이블
# ─────────────────────────────────────────────────────

# ECMWF HRES 예보 테이블 — 바람(u10, v10) 전용
# Phase 14-B PK 재설계:
#   - 구 PK: (issued_at, datetime, lat, lon) → 매일 새 행이 추가되어 과거 예보가 누적됨
#   - 신 PK: (datetime, lat, lon)            → 같은 유효시각에 최신 예보로 UPSERT (덮어쓰기)
#   - issued_at: 일반 컬럼으로 유지 (발행 시각 정보 보존, 대시보드 조회용)
#
# ※ 파랑 변수는 env_noaa_forecast 테이블에서 전담
#   (ECMWF Open Data 무료 tier에서 너울·풍파 6개 변수 미제공 확인 2026-03-17)
SQL_CREATE_ECMWF_FORECAST_TABLE = """
CREATE TABLE IF NOT EXISTS env_ecmwf_forecast (
    datetime  TIMESTAMPTZ NOT NULL,  -- 예보 유효 시각 (valid time, UTC) ← PK
    lat       REAL        NOT NULL,  -- 위도 ← PK
    lon       REAL        NOT NULL,  -- 경도 ← PK
    issued_at TIMESTAMPTZ NOT NULL,  -- 예보 발행 시각 (run time, UTC) ← 일반 컬럼

    -- 바람(Wind) 변수 (ecmwf_fc_wind_* 파일에서 채워짐)
    u10   REAL,   -- 10m 동서 풍속 (m/s)
    v10   REAL,   -- 10m 남북 풍속 (m/s)

    -- 복합 기본키: 동일 유효시각+위치 → 최신 예보로 UPSERT
    PRIMARY KEY (datetime, lat, lon)
);
"""

# ECMWF 예보 Hypertable 변환 (datetime 기준 시계열 최적화)
SQL_CREATE_ECMWF_FORECAST_HYPERTABLE = """
SELECT create_hypertable(
    'env_ecmwf_forecast',
    'datetime',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);
"""

# ECMWF 예보 위치 기반 인덱스
SQL_CREATE_ECMWF_FORECAST_INDEX = """
CREATE INDEX IF NOT EXISTS idx_ecmwf_forecast_latlon
    ON env_ecmwf_forecast (lat, lon);
"""

# ECMWF 예보 issued_at 인덱스 (대시보드에서 최신 발행 시각 조회용)
SQL_CREATE_ECMWF_FORECAST_ISSUED_INDEX = """
CREATE INDEX IF NOT EXISTS idx_ecmwf_forecast_issued
    ON env_ecmwf_forecast (issued_at DESC);
"""


# ─────────────────────────────────────────────────────
# HYCOM 예보 데이터 테이블
# ─────────────────────────────────────────────────────

# HYCOM 예보 테이블 (Phase 14-B PK 재설계 적용)
# 신 PK: (datetime, lat, lon) → 같은 유효시각에 최신 예보로 UPSERT
SQL_CREATE_HYCOM_FORECAST_TABLE = """
CREATE TABLE IF NOT EXISTS env_hycom_forecast (
    datetime  TIMESTAMPTZ NOT NULL,  -- 예보 유효 시각 (3시간 간격) ← PK
    lat       REAL        NOT NULL,  -- 위도 ← PK
    lon       REAL        NOT NULL,  -- 경도 ← PK
    issued_at TIMESTAMPTZ NOT NULL,  -- 예보 실행일 (다운로드 날짜, UTC) ← 일반 컬럼

    water_u   REAL,   -- 동서 해류 속도 (m/s)
    water_v   REAL,   -- 남북 해류 속도 (m/s)

    PRIMARY KEY (datetime, lat, lon)
);
"""

# HYCOM 예보 Hypertable 변환
SQL_CREATE_HYCOM_FORECAST_HYPERTABLE = """
SELECT create_hypertable(
    'env_hycom_forecast',
    'datetime',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);
"""

# HYCOM 예보 issued_at 인덱스
SQL_CREATE_HYCOM_FORECAST_ISSUED_INDEX = """
CREATE INDEX IF NOT EXISTS idx_hycom_forecast_issued
    ON env_hycom_forecast (issued_at DESC);
"""


# ─────────────────────────────────────────────────────
# NOAA WW3 파랑 예보 데이터 테이블
# ─────────────────────────────────────────────────────

# NOAA WaveWatch III 파랑 예보 테이블 (Phase 14-B PK 재설계 적용)
# 신 PK: (datetime, lat, lon) → 같은 유효시각에 최신 예보로 UPSERT
# 변수 출처: PacIOOS ERDDAP ww3_global
# 컬럼명은 ECMWF wave 컬럼명과 동일하게 통일 (LLM 쿼리 일관성)
SQL_CREATE_NOAA_FORECAST_TABLE = """
CREATE TABLE IF NOT EXISTS env_noaa_forecast (
    datetime  TIMESTAMPTZ NOT NULL,  -- 예보 유효 시각 ← PK
    lat       REAL        NOT NULL,  -- 위도 (-77.5 ~ 77.5, 0.5° 간격) ← PK
    lon       REAL        NOT NULL,  -- 경도 (-180 ~ 180, 0.5° 간격) ← PK
    issued_at TIMESTAMPTZ NOT NULL,  -- 예보 실행일 (다운로드 날짜, UTC) ← 일반 컬럼

    -- 파랑 변수 9개 (NOAA WW3 → ECMWF 컬럼명으로 통일)
    -- 복합파 (풍파 + 너울 합산)
    swh   REAL,   -- 복합 유의파고 (m)    ← Thgt
    mwd   REAL,   -- 복합 파랑 방향 (°)  ← Tdir
    mwp   REAL,   -- 복합 파랑 주기 (s)  ← Tper

    -- 너울 (원거리 폭풍에 의한 긴 파)
    shts  REAL,   -- 너울 유의파고 (m)   ← shgt
    mdts  REAL,   -- 너울 방향 (°)      ← sdir
    mpts  REAL,   -- 너울 주기 (s)      ← sper

    -- 풍파 (현지 바람에 의한 짧은 파)
    shww  REAL,   -- 풍파 유의파고 (m)   ← whgt
    mdww  REAL,   -- 풍파 방향 (°)      ← wdir
    mpww  REAL,   -- 풍파 주기 (s)      ← wper

    -- 복합 기본키: 동일 유효시각+위치 → 최신 예보로 UPSERT
    PRIMARY KEY (datetime, lat, lon)
);
"""

# NOAA 예보 Hypertable 변환
SQL_CREATE_NOAA_FORECAST_HYPERTABLE = """
SELECT create_hypertable(
    'env_noaa_forecast',
    'datetime',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);
"""

# NOAA 예보 위치 기반 인덱스
SQL_CREATE_NOAA_FORECAST_LATLON_INDEX = """
CREATE INDEX IF NOT EXISTS idx_noaa_forecast_latlon
    ON env_noaa_forecast (lat, lon);
"""

# NOAA 예보 최신 예보 조회용 issued_at 인덱스
SQL_CREATE_NOAA_FORECAST_ISSUED_INDEX = """
CREATE INDEX IF NOT EXISTS idx_noaa_forecast_issued
    ON env_noaa_forecast (issued_at DESC);
"""


# ─────────────────────────────────────────────────────
# 파이프라인 상태 추적 테이블 (Phase 10 신규)
# ─────────────────────────────────────────────────────

# 파이프라인이 스스로 적재 상태를 추적하기 위한 메타 테이블
# 각 날짜 × 소스 조합별 상태를 기록하여
# 누락 감지, 자동 백필, 예보→재분석 교체 판단의 근거로 사용
SQL_CREATE_COVERAGE_TABLE = """
CREATE TABLE IF NOT EXISTS pipeline_coverage (
    date          DATE        NOT NULL,          -- 데이터 날짜 (UTC 기준 하루)
    source        TEXT        NOT NULL,          -- 데이터 소스 종류
    -- source 가능값:
    --   'ecmwf_reanalysis' : ERA5/ERA5T 재분석 (바람+파랑 통합)
    --   'hycom_current'    : HYCOM 해류 분석
    --   'ecmwf_forecast'   : ECMWF 바람 예보
    --   'noaa_forecast'    : NOAA WW3 파랑 예보
    --   'hycom_forecast'   : HYCOM 해류 예보

    status        TEXT        NOT NULL DEFAULT 'missing',
    -- status 가능값:
    --   'complete'           : DB 적재 완전 완료
    --   'partial'            : 일부만 적재됨 (재시도 대상)
    --   'forecast_only'      : 재분석 미제공 → 예보로 임시 커버
    --   'permanent_forecast' : HYCOM 롤링 윈도우 초과 → 예보가 영구 확정
    --   'missing'            : 어떤 데이터도 없음 (기본값)
    --   'failed'             : 다운로드/적재 시도 중 오류 발생

    data_type     TEXT,                          -- 'era5' 또는 'era5t' (재분석 품질 구분용)
    row_count     INTEGER,                       -- 실제 적재된 행 수
    expected_rows INTEGER,                       -- 기대 행 수 (partial 판정 기준)
    loaded_at     TIMESTAMPTZ,                   -- 마지막 적재/갱신 시각 (UTC)
    notes         TEXT,                          -- 이상 상황 메모 (예: "HYCOM window expired")

    PRIMARY KEY (date, source)                   -- 날짜+소스 조합으로 유일성 보장
);
"""

# pipeline_coverage 날짜 기반 인덱스 (기간 범위 조회 성능)
SQL_CREATE_COVERAGE_DATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_coverage_date
    ON pipeline_coverage (date DESC);
"""

# pipeline_coverage 상태 기반 인덱스 (누락/실패 날짜 빠른 조회)
SQL_CREATE_COVERAGE_STATUS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_coverage_status
    ON pipeline_coverage (status, source);
"""


# ─────────────────────────────────────────────────────
# 예보 테이블 DROP SQL (Phase 14-B 마이그레이션용)
# ─────────────────────────────────────────────────────

# 예보 테이블 3개를 CASCADE로 삭제 (Hypertable + 인덱스 포함)
# ⚠️ 데이터가 있는 경우 모두 삭제됨 — 마이그레이션 전 반드시 확인
SQL_DROP_FORECAST_TABLES = """
DROP TABLE IF EXISTS env_ecmwf_forecast CASCADE;
DROP TABLE IF EXISTS env_hycom_forecast  CASCADE;
DROP TABLE IF EXISTS env_noaa_forecast   CASCADE;
"""


def reinit_forecast_tables() -> None:
    """
    예보 테이블 3개를 삭제 후 신규 스키마로 재생성 (Phase 14-B 마이그레이션)

    변경 내용:
      - 구 PK: (issued_at, datetime, lat, lon)
      - 신 PK: (datetime, lat, lon) + issued_at 일반 컬럼

    ⚠️ 주의: 기존 예보 데이터가 모두 삭제됩니다.
         재분석/분석 테이블(env_ecmwf_reanalysis, env_hycom_current)은 영향 없음.

    실행 방법:
      uv run python run.py --reinit-forecast
    """
    logger.warning(
        "⚠️ 예보 테이블 스키마 마이그레이션 시작 "
        "(env_ecmwf_forecast / env_hycom_forecast / env_noaa_forecast)"
    )
    conn = get_connection()
    try:
        cursor = conn.cursor()

        # ── 1단계: 기존 예보 테이블 삭제 ──
        logger.info("  예보 테이블 3개 삭제 중 (CASCADE)...")
        cursor.execute(SQL_DROP_FORECAST_TABLES)

        # ── 2단계: ECMWF 예보 테이블 재생성 ──
        logger.info("  ECMWF 예보 테이블 재생성 중...")
        cursor.execute(SQL_CREATE_ECMWF_FORECAST_TABLE)
        cursor.execute(SQL_CREATE_ECMWF_FORECAST_HYPERTABLE)
        cursor.execute(SQL_CREATE_ECMWF_FORECAST_INDEX)
        cursor.execute(SQL_CREATE_ECMWF_FORECAST_ISSUED_INDEX)

        # ── 3단계: HYCOM 예보 테이블 재생성 ──
        logger.info("  HYCOM 예보 테이블 재생성 중...")
        cursor.execute(SQL_CREATE_HYCOM_FORECAST_TABLE)
        cursor.execute(SQL_CREATE_HYCOM_FORECAST_HYPERTABLE)
        cursor.execute(SQL_CREATE_HYCOM_FORECAST_ISSUED_INDEX)

        # ── 4단계: NOAA WW3 예보 테이블 재생성 ──
        logger.info("  NOAA WW3 예보 테이블 재생성 중...")
        cursor.execute(SQL_CREATE_NOAA_FORECAST_TABLE)
        cursor.execute(SQL_CREATE_NOAA_FORECAST_HYPERTABLE)
        cursor.execute(SQL_CREATE_NOAA_FORECAST_LATLON_INDEX)
        cursor.execute(SQL_CREATE_NOAA_FORECAST_ISSUED_INDEX)

        conn.commit()
        cursor.close()
        logger.success(
            "예보 테이블 스키마 마이그레이션 완료 "
            "(신 PK: datetime, lat, lon / issued_at → 일반 컬럼)"
        )

    except Exception as e:
        conn.rollback()
        logger.error(f"예보 테이블 마이그레이션 실패: {e}")
        raise
    finally:
        conn.close()


def initialize_schema() -> None:
    """
    DB 스키마 초기화 (최초 1회 실행)

    다음을 순서대로 실행:
     1.  TimescaleDB 확장 설치
     2.  ECMWF 재분석 테이블 생성 + Hypertable + 인덱스 + 압축
     3.  HYCOM 분석 테이블 생성 + Hypertable + 압축
     4.  ECMWF 예보 테이블 생성 + Hypertable + 인덱스
     5.  HYCOM 예보 테이블 생성 + Hypertable + 인덱스
     6.  NOAA WW3 파랑 예보 테이블 생성 + Hypertable + 인덱스
     7.  pipeline_coverage 테이블 생성 + 인덱스 (Phase 10 신규)
    """
    logger.info("DB 스키마 초기화 시작...")

    # DB에 연결
    conn = get_connection()

    try:
        cursor = conn.cursor()

        # ── 1단계: TimescaleDB 확장 활성화 ──
        logger.info("  1/15 TimescaleDB 확장 설치 중...")
        cursor.execute(SQL_CREATE_EXTENSION)

        # ── 2단계: ECMWF 재분석 테이블 생성 ──
        logger.info("  2/15 ECMWF 재분석 테이블 생성 중...")
        cursor.execute(SQL_CREATE_TABLE)

        # ── 3단계: ECMWF 재분석 Hypertable 변환 ──
        logger.info("  3/15 ECMWF 재분석 Hypertable 변환 중...")
        cursor.execute(SQL_CREATE_HYPERTABLE)

        # ── 4단계: ECMWF 재분석 인덱스 생성 ──
        logger.info("  4/15 ECMWF 재분석 인덱스 생성 중...")
        cursor.execute(SQL_CREATE_INDEX)

        # ── 5단계: HYCOM 분석 테이블 생성 ──
        logger.info("  5/15 HYCOM 분석 테이블 생성 중...")
        cursor.execute(SQL_CREATE_HYCOM_TABLE)

        # ── 6단계: HYCOM 분석 Hypertable 변환 ──
        logger.info("  6/15 HYCOM 분석 Hypertable 변환 중...")
        cursor.execute(SQL_CREATE_HYCOM_HYPERTABLE)

        # ── 7단계: 자동 압축 정책 (재분석/분석 테이블) ──
        # 압축 활성화를 먼저 실행한 뒤 정책 추가해야 함 (TimescaleDB 요구 순서)
        logger.info("  7/15 재분석/분석 테이블 압축 활성화 + 정책 설정 중...")
        cursor.execute(SQL_ENABLE_ECMWF_COMPRESSION)
        cursor.execute(SQL_COMPRESSION_POLICY)
        cursor.execute(SQL_ENABLE_HYCOM_COMPRESSION)
        cursor.execute(SQL_HYCOM_COMPRESSION_POLICY)

        # ── 8단계: ECMWF 예보 테이블 생성 ──
        logger.info("  8/15 ECMWF 예보 테이블 생성 중...")
        cursor.execute(SQL_CREATE_ECMWF_FORECAST_TABLE)

        # ── 9단계: ECMWF 예보 Hypertable 변환 + 인덱스 ──
        logger.info("  9/15 ECMWF 예보 Hypertable + 인덱스 생성 중...")
        cursor.execute(SQL_CREATE_ECMWF_FORECAST_HYPERTABLE)
        cursor.execute(SQL_CREATE_ECMWF_FORECAST_INDEX)
        cursor.execute(SQL_CREATE_ECMWF_FORECAST_ISSUED_INDEX)

        # ── 10단계: HYCOM 예보 테이블 생성 ──
        logger.info("  10/15 HYCOM 예보 테이블 생성 중...")
        cursor.execute(SQL_CREATE_HYCOM_FORECAST_TABLE)

        # ── 11단계: HYCOM 예보 Hypertable + 인덱스 ──
        logger.info("  11/15 HYCOM 예보 Hypertable + 인덱스 생성 중...")
        cursor.execute(SQL_CREATE_HYCOM_FORECAST_HYPERTABLE)
        cursor.execute(SQL_CREATE_HYCOM_FORECAST_ISSUED_INDEX)

        # ── 12단계: NOAA WW3 파랑 예보 테이블 생성 ──
        logger.info("  12/15 NOAA WW3 파랑 예보 테이블 생성 중...")
        cursor.execute(SQL_CREATE_NOAA_FORECAST_TABLE)

        # ── 13단계: NOAA WW3 예보 Hypertable + 인덱스 ──
        logger.info("  13/15 NOAA WW3 예보 Hypertable + 인덱스 생성 중...")
        cursor.execute(SQL_CREATE_NOAA_FORECAST_HYPERTABLE)
        cursor.execute(SQL_CREATE_NOAA_FORECAST_LATLON_INDEX)
        cursor.execute(SQL_CREATE_NOAA_FORECAST_ISSUED_INDEX)

        # ── 14단계: pipeline_coverage 테이블 생성 (Phase 10 신규) ──
        logger.info("  14/15 pipeline_coverage 테이블 생성 중...")
        cursor.execute(SQL_CREATE_COVERAGE_TABLE)

        # ── 15단계: pipeline_coverage 인덱스 생성 ──
        logger.info("  15/15 pipeline_coverage 인덱스 생성 중...")
        cursor.execute(SQL_CREATE_COVERAGE_DATE_INDEX)
        cursor.execute(SQL_CREATE_COVERAGE_STATUS_INDEX)

        # 모든 변경사항을 DB에 확정 반영
        conn.commit()
        cursor.close()

        logger.success(
            "DB 스키마 초기화 완료! "
            "(데이터 테이블 5개 + pipeline_coverage 메타 테이블 1개)"
        )

    except Exception as e:
        # 에러 발생 시 변경사항 모두 취소
        conn.rollback()
        logger.error(f"스키마 초기화 실패: {e}")
        raise

    finally:
        # 성공/실패 여부에 관계없이 반드시 연결 닫기
        conn.close()
