# DB 스키마 명세서

## 테이블: env_ecmwf_reanalysis

ECMWF ERA5 재분석 데이터 저장 테이블

### 컬럼 정의

| 컬럼명 | 타입 | NOT NULL | 설명 |
|--------|------|----------|------|
| datetime | TIMESTAMPTZ | ✅ | 관측 시각 (UTC 기준 타임존 포함) |
| lat | REAL | ✅ | 위도 (-90 ~ 90, 북쪽 +) |
| lon | REAL | ✅ | 경도 (-180 ~ 180, 동쪽 +) |
| swh | REAL | | 복합 유의파고 (m) |
| mwd | REAL | | 평균 파랑 방향 (°) |
| mwp | REAL | | 평균 파랑 주기 (s) |
| shts | REAL | | 너울 유의파고 (m) |
| mdts | REAL | | 너울 방향 (°) |
| mpts | REAL | | 너울 주기 (s) |
| shww | REAL | | 풍파 유의파고 (m) |
| mdww | REAL | | 풍파 방향 (°) |
| mpww | REAL | | 풍파 주기 (s) |
| u10 | REAL | | 동서 풍속 10m (m/s) |
| v10 | REAL | | 남북 풍속 10m (m/s) |

### 기본 키 (PK)
- `(datetime, lat, lon)` — 같은 시각, 같은 위치의 중복 적재 방지

### TimescaleDB 설정
- **Hypertable 기준 컬럼**: `datetime`
- **청크 간격**: 7일 (7일치 데이터를 하나의 청크로 관리)
- **압축**: 청크 나이 30일 이후 자동 압축 → 용량 50~80% 절감

### 인덱스
- `(lat, lon)` — 특정 위치의 전체 시계열 조회 최적화
- `datetime` — Hypertable 기본 인덱스 (자동 생성)

### 생성 SQL

```sql
-- TimescaleDB 확장 활성화
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 메인 테이블 생성
CREATE TABLE IF NOT EXISTS env_ecmwf_reanalysis (
    datetime  TIMESTAMPTZ NOT NULL,
    lat       REAL        NOT NULL,
    lon       REAL        NOT NULL,
    swh       REAL,   -- 복합 유의파고 (m)
    mwd       REAL,   -- 평균 파랑 방향 (°)
    mwp       REAL,   -- 평균 파랑 주기 (s)
    shts      REAL,   -- 너울 유의파고 (m)
    mdts      REAL,   -- 너울 방향 (°)
    mpts      REAL,   -- 너울 주기 (s)
    shww      REAL,   -- 풍파 유의파고 (m)
    mdww      REAL,   -- 풍파 방향 (°)
    mpww      REAL,   -- 풍파 주기 (s)
    u10       REAL,   -- 동서 풍속 (m/s)
    v10       REAL,   -- 남북 풍속 (m/s)
    PRIMARY KEY (datetime, lat, lon)
);

-- TimescaleDB Hypertable 변환 (시계열 최적화)
SELECT create_hypertable(
    'env_ecmwf_reanalysis',
    'datetime',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- 위치 기반 쿼리 인덱스
CREATE INDEX IF NOT EXISTS idx_ecmwf_reanalysis_latlon
    ON env_ecmwf_reanalysis (lat, lon);

-- 자동 압축 정책 (30일 지난 청크 압축)
SELECT add_compression_policy(
    'env_ecmwf_reanalysis',
    INTERVAL '30 days'
);
```

### 조회 예시

```sql
-- 특정 위치(부산항 근처)의 최근 24시간 파고 조회
SELECT datetime, swh, mwd, mwp, u10, v10
FROM env_ecmwf_reanalysis
WHERE lat BETWEEN 34.5 AND 35.5
  AND lon BETWEEN 128.5 AND 129.5
  AND datetime >= NOW() - INTERVAL '24 hours'
ORDER BY datetime;
```
