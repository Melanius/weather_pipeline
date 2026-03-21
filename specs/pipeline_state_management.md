# 파이프라인 상태 관리 명세서

> **작성일**: 2026-03-18
> **목적**: 데이터 누락 없음, 중복 없음, 예보→재분석 자동 교체를 보장하는 견고한 파이프라인 설계
> **관련 파일**: `src/env_pipeline/db/schema.py`, `src/env_pipeline/pipeline.py`, `config/settings.toml`, `config/down.json`

---

## 1. 설계 원칙

| 원칙 | 설명 |
|------|------|
| **누락 없음** | 어떤 돌발 상황(서버 장애, 제공업체 지연)에서도 데이터 공백이 생기지 않아야 함 |
| **중복 없음** | 동일 시각·위치에 재분석과 예보가 공존하지 않음. 단일 진실(Single Source of Truth) 유지 |
| **자동 교체** | 예보로 임시 채운 구간에 재분석이 제공되면 예보를 삭제하고 재분석으로 교체 |
| **DB 독립 다운로드** | `download_only` 모드는 DB 없이 실행 가능. 상태 추적은 로컬 파일 기반 |
| **자가 진단** | 파이프라인이 매 실행 시 스스로 누락 구간을 감지하고 백필 시도 |

---

## 2. 데이터 우선순위 (동일 기간 데이터 충돌 시)

```
ERA5 최종확정  >  ERA5T (근실시간)  >  예보(ECMWF/NOAA/HYCOM)
```

- ERA5 최종확정: 오늘 기준 약 5~8일 이상 지연 제공 (ECMWF 사정에 따라 가변)
- ERA5T: 오늘 기준 약 2~3일 이연 제공
- 예보: 오늘 기준 D+5~D+10 (공백 구간 임시 커버용)

---

## 3. 날짜 구성 설정

### 3-1. settings.toml (상시 파이프라인 동작 기준)

```toml
[pipeline]
coverage_lookback_days = 30   # 상태 진단 시 오늘 기준 몇 일 전까지 스캔할지
era5_delay_days        = 7    # ERA5 제공 지연 기본값 (실제 가용 여부는 API로 확인)
era5t_delay_days       = 3    # ERA5T 제공 지연 기본값
hycom_window_days      = 10   # HYCOM 롤링 윈도우 (이 기간 초과 시 영구 소실 경고)
```

> **핵심**: N일(몇 일 전까지 스캔할지)은 `settings.toml`의 `coverage_lookback_days`에서 설정.
> `down.json`은 자동 모드에서 사용되지 않고 수동 백필 전용으로 역할이 재정의됨.

### 3-2. down.json (수동 백필 전용)

```json
{
  "_comment": "자동 실행 시에는 무시됨. 수동으로 특정 기간 백필이 필요할 때만 사용.",
  "manual_start": "2026-01-01",
  "manual_end":   "2026-03-01"
}
```

> **언제 사용하나**: 과거 전체 데이터(2021~) 일괄 적재 등 특수 상황에서만 `--manual` 플래그와 함께 사용.

---

## 4. pipeline_coverage 테이블 (신규)

파이프라인이 스스로 상태를 추적하기 위한 메타 테이블.

### 4-1. 스키마

```sql
CREATE TABLE IF NOT EXISTS pipeline_coverage (
    date        DATE        NOT NULL,
    source      TEXT        NOT NULL,
    -- 소스 종류:
    --   'ecmwf_reanalysis'  : ERA5/ERA5T 재분석 (바람+파랑)
    --   'hycom_current'     : HYCOM 해류 분석
    --   'ecmwf_forecast'    : ECMWF 바람 예보
    --   'noaa_forecast'     : NOAA WW3 파랑 예보
    --   'hycom_forecast'    : HYCOM 해류 예보
    status      TEXT        NOT NULL DEFAULT 'missing',
    -- 상태 종류:
    --   'complete'           : 해당 날짜 DB 적재 완전 완료
    --   'partial'            : 일부만 적재됨 (재시도 대상)
    --   'forecast_only'      : 재분석 미제공 → 예보로 임시 커버
    --   'permanent_forecast' : HYCOM 윈도우 초과 → 예보가 영구 확정 (cleanup 제외)
    --   'missing'            : 어떤 데이터도 없음
    --   'failed'             : 다운로드/적재 실패
    data_type   TEXT,
    -- 적재된 데이터 종류:
    --   'era5', 'era5t'     : 재분석 품질 구분 (ERA5T → ERA5 교체 추적용)
    --   NULL                : 예보 소스는 구분 불필요
    row_count   INTEGER,               -- 실제 적재된 행 수 (partial 감지용)
    expected_rows INTEGER,             -- 기대 행 수 (날짜별 격자점×시간 스텝)
    loaded_at   TIMESTAMPTZ,           -- 마지막 적재 시각
    notes       TEXT,                  -- 이상 상황 메모 (예: "HYCOM window expired")
    PRIMARY KEY (date, source)
);
```

### 4-2. 상태 전이 다이어그램

```
[missing]
    │
    ├─ 재분석 제공 확인 → 다운로드/적재 성공 → [complete / era5t]
    │                                  └─ 나중에 ERA5 최종 확인 → [complete / era5]
    │
    ├─ 재분석 미제공 + 예보 적재 성공 → [forecast_only]
    │       └─ 이후 재분석 제공 → 예보 cleanup + 재분석 적재 → [complete]
    │
    ├─ HYCOM 윈도우 초과 경고 발생 → [permanent_forecast] ← cleanup 대상 제외
    │
    ├─ 적재 중 오류 → [failed] → 다음 실행 시 재시도
    │
    └─ 부분 적재(row_count < expected_rows * 0.95) → [partial] → 재시도 대상
```

---

## 5. 파이프라인 실행 흐름 (재설계)

### 5-1. download_only 모드 (DB 독립)

```
[download_only 실행]
    │
    ├─ [Step D1] 로컬 data/ 폴더 스캔
    │      → 날짜별 NC 파일 존재 여부로 다운로드 필요 여부 판단
    │      → DB/pipeline_coverage 참조 없음 (DB 없이도 실행 가능)
    │
    ├─ [Step D2] 제공업체 실제 가용 범위 조회
    │      → CDS API: 실제로 제공 가능한 최신 날짜 확인
    │      → HYCOM: 롤링 윈도우 범위 확인
    │      → "오늘 - N일" 고정값 사용 금지
    │
    └─ [Step D3] 가용 확인된 날짜만 다운로드
           → 파일 이미 있으면 건너뜀 (기존 동작 유지)
           → 실패 시 로그 기록 후 다음 날짜 계속 진행
```

### 5-2. load_only / full / forecast 모드 (DB 필요)

```
[적재 모드 실행]
    │
    ├─ [Step L1] 상태 진단 (pipeline_coverage 스캔)
    │      → 오늘 기준 coverage_lookback_days 범위 스캔
    │      → 'missing', 'partial', 'failed' 날짜 목록 추출 (백필 대상)
    │      → 'forecast_only' 날짜 중 재분석이 새로 제공된 것 확인 (교체 대상)
    │
    ├─ [Step L2] 재분석 적재
    │      → data/ 폴더의 NC 파일 → DB COPY 적재
    │      → 적재 완료 후 row_count 검증
    │         - row_count ≥ expected_rows × 0.95 → status = 'complete'
    │         - row_count < expected_rows × 0.95 → status = 'partial' (재시도 대상)
    │      → ERA5T/ERA5 구분 기록 (data_type 컬럼)
    │
    ├─ [Step L3] ERA5T → ERA5 교체 처리 (UPSERT)
    │      → pipeline_coverage에서 data_type='era5t' 날짜 조회
    │      → 해당 날짜에 ERA5 최종 파일이 새로 다운로드되었으면
    │         → env_ecmwf_reanalysis에 UPSERT (ON CONFLICT UPDATE)
    │         → pipeline_coverage.data_type = 'era5' 로 갱신
    │
    ├─ [Step L4] Transactional Cleanup (예보 → 재분석 교체)
    │      → Step L2에서 status='complete'가 된 날짜에 대해서만 실행
    │      → 'partial'/'failed' 날짜는 cleanup 보류 (예보 유지)
    │      → 삭제 순서:
    │         1. DELETE FROM env_ecmwf_forecast WHERE date(datetime) = 해당 날짜
    │         2. DELETE FROM env_noaa_forecast  WHERE date(datetime) = 해당 날짜
    │         3. DELETE FROM env_hycom_forecast WHERE date(datetime) = 해당 날짜
    │      → status가 'permanent_forecast'인 날짜는 cleanup 제외
    │
    ├─ [Step L5] 예보 적재 (공백 구간 커버)
    │      → 재분석 미제공 구간 + forecast_only 구간에 예보 적재
    │      → pipeline_coverage.status = 'forecast_only' 기록
    │
    └─ [Step L6] 이상 감지 및 경고 로그
           → HYCOM 윈도우 초과 날짜 감지
              - 'missing' + HYCOM 날짜 > hycom_window_days → 'permanent_forecast' 승격
              - 경고 로그: "HYCOM analysis data permanently unavailable for YYYY-MM-DD"
           → N일 이상 'missing'/'failed' 지속 날짜 경고
           → 예보 제공업체 장애 감지 (예보도 없는 미래 날짜 존재 시)
```

---

## 6. 핵심 예외 처리 시나리오

### 6-1. ERA5 제공 지연 (8일 이상 지연)

| 상황 | 대응 |
|------|------|
| CDS API 조회 → 해당 날짜 미제공 확인 | 다운로드 시도 안 함. pipeline_coverage 상태 유지 |
| 나중에 제공 시 | 다음 실행 시 Step D2에서 감지 → 자동 다운로드 → 적재 → cleanup |
| 오래 지연될 경우 | forecast_only 상태 유지. 경고 로그만 출력 |

> **핵심**: `-7일 고정` 방식 대신 CDS API에 실제로 가용 여부를 물어보는 방식으로 변경.

### 6-2. 서버 10일 이상 장애

| 상황 | 대응 |
|------|------|
| 장애 기간 data/ 폴더 파일 없음 | Step L1 상태 진단에서 전체 구간 'missing' 감지 |
| ERA5는 백필 가능 | coverage_lookback_days 범위 내 누락 날짜 자동 재다운로드 시도 |
| HYCOM 분석은 10일 초과 시 영구 소실 | Step L6에서 'permanent_forecast'로 승격 + 경고 |
| 10일 내 HYCOM 분석 백필 | 정상 백필 가능 (HYCOM 윈도우 내) |

### 6-3. 부분 적재(Partial Load)

```
예상 행 수 계산 기준 (소스별):
  ecmwf_reanalysis : 24시간 × 위도 격자 × 경도 격자 (0.25° 해상도)
  hycom_current    : 8스텝(3h간격) × HYCOM 격자 (stride=3)
  noaa_forecast    : 48스텝(1h간격) × WW3 격자 (0.5° 해상도)

판정 기준:
  실제 행 수 ≥ 예상 행 수 × 0.95 → 'complete' (5% 미만 NaN/육지 제외 허용)
  실제 행 수 < 예상 행 수 × 0.95 → 'partial'  → 다음 실행 시 재시도
```

### 6-4. HYCOM 영구 소실 처리

```
감지 조건:
  source = 'hycom_current'
  status = 'missing' or 'failed'
  오늘 날짜 - date > hycom_window_days (기본 10일)

처리:
  → status = 'permanent_forecast' 로 변경
  → notes = "HYCOM analysis window expired. Forecast data retained permanently."
  → 해당 날짜의 env_hycom_forecast 데이터는 cleanup 대상에서 영구 제외
  → 경고 로그 출력
```

---

## 7. 설정 파일 변경 요약

### settings.toml 추가 항목

```toml
[pipeline]
coverage_lookback_days = 30   # 상태 진단 범위 (일)
era5_delay_days        = 7    # ERA5 기본 지연 기본값 (실제 가용 여부는 API 확인)
era5t_delay_days       = 3    # ERA5T 기본 지연 기본값
hycom_window_days      = 10   # HYCOM 분석 보유 롤링 윈도우 (일)
partial_threshold      = 0.95 # 이 비율 미만이면 partial로 판정

[pipeline.alerts]
log_missing_days_threshold = 3   # N일 이상 missing/failed 지속 시 경고 출력
```

### down.json 역할 재정의

```json
{
  "_comment": "자동 실행 시에는 무시됨. --manual 플래그와 함께 사용하는 수동 백필 전용.",
  "manual_start": null,
  "manual_end":   null
}
```

---

## 8. run.py CLI 옵션 추가 계획

| 옵션 | 설명 |
|------|------|
| `--mode download_only` | 기존 유지. DB 없이 파일만 다운로드 |
| `--mode load_only` | 기존 유지. 상태 진단 + 재분석 적재 + cleanup |
| `--mode forecast` | 기존 유지. 예보 적재 |
| `--mode full` | 기존 유지. 전체 실행 |
| `--manual` | down.json의 manual_start/end 기준으로 강제 실행 (과거 백필용) |
| `--diagnose` | 실행 없이 pipeline_coverage 현재 상태만 출력 (테이블 조회만, 변경 없음) |
| `--simulate-date YYYY-MM-DD` | "오늘"을 지정 날짜로 가정하고 실행 (테스트 전용) |
| `--dry-run` | 실제 DB 변경 없이 "무엇을 할 것인지"만 출력 (단독 또는 --simulate-date와 조합) |

---

## 9. 테스트 시나리오 (--simulate-date 활용)

실제 스케줄러는 1일 1회 실행이므로 로직 검증에 수일이 소요됨.
`--simulate-date`로 "오늘"을 과거 날짜로 주입하면 분 단위로 여러 날 사이클을 테스트 가능.

### 9-1. 기본 사이클 테스트

```bash
# Day 1 시뮬레이션: 3/10이 오늘인 것처럼 실행
# → 재분석 3/1~3/5 적재, 3/6~3/10 공백 → 예보로 채움
uv run python run.py --mode full --simulate-date 2026-03-10

# Day 2 시뮬레이션: 3/11이 오늘 (재분석 3/6~3/8 새로 제공 가정)
# → 재분석 3/6~3/8 적재 완료 → cleanup: 예보 테이블에서 3/6~3/8 DELETE 확인
uv run python run.py --mode full --simulate-date 2026-03-11

# Day 3 시뮬레이션: 3/20이 오늘 (HYCOM 윈도우 초과 확인)
# → HYCOM missing 날짜가 10일 초과 → permanent_forecast 승격 경고 확인
uv run python run.py --mode full --simulate-date 2026-03-20
```

### 9-2. dry-run 조합 (DB 변경 없이 미리 확인)

```bash
# 실제 실행 전 "무엇을 삭제할 것인지" 미리 확인
uv run python run.py --mode full --simulate-date 2026-03-11 --dry-run

# 현재 coverage 상태만 출력
uv run python run.py --diagnose
```

### 9-3. 서버 장애 복구 시나리오 테스트

```bash
# 3/5~3/15 기간 아무것도 적재 안 된 상태에서 복구 시뮬레이션
uv run python run.py --diagnose  # missing 구간 확인

# 복구 실행 (lookback_days 범위 내 자동 백필)
uv run python run.py --mode full --simulate-date 2026-03-15
```

---

## 10. 미구현 항목 (TODO)

### Phase 10-A: pipeline_coverage 인프라
- [ ] `pipeline_coverage` 테이블 스키마 추가 (`src/env_pipeline/db/schema.py`)
- [ ] 예상 행 수(expected_rows) 계산 로직 구현 (소스별 격자점×시간 스텝)
- [ ] `--init-db` 시 pipeline_coverage 테이블도 함께 생성

### Phase 10-B: 상태 진단 엔진
- [ ] `src/env_pipeline/db/coverage.py` 신규 모듈 생성
  - `diagnose()`: pipeline_coverage 스캔 → missing/partial/failed 날짜 목록 반환
  - `update_coverage()`: 적재 완료 후 상태 갱신
  - `get_backfill_dates()`: 백필 필요 날짜 목록 반환
- [ ] CDS API 실제 가용 날짜 조회 함수 구현 (고정 -7일 대신 API 응답 기반)
- [ ] HYCOM 롤링 윈도우 범위 확인 함수 구현

### Phase 10-C: Cleanup 및 교체 로직
- [ ] `cleanup_superseded_forecasts(dates)` 함수 구현
  - status='complete' 날짜에 대해서만 예보 DELETE
  - status='permanent_forecast' 날짜는 제외
- [ ] ERA5T → ERA5 UPSERT 로직 구현 (ON CONFLICT UPDATE)
- [ ] HYCOM 윈도우 초과 날짜 → permanent_forecast 승격 로직

### Phase 10-D: CLI 확장
- [ ] `--diagnose` 옵션 구현 (DB 변경 없이 현재 상태 출력)
- [ ] `--simulate-date YYYY-MM-DD` 옵션 구현 (테스트용 "오늘" 주입)
- [ ] `--dry-run` 옵션 구현 (실행 계획만 출력)
- [ ] `--manual` 옵션 구현 (down.json 기반 수동 백필)

### Phase 10-E: settings.toml 확장
- [ ] `[pipeline]` 섹션 추가 (coverage_lookback_days, hycom_window_days 등)

### Phase 10-F: 스케줄러
- [ ] Windows 작업 스케줄러 설정 가이드 작성 (WSL 환경)
- [ ] Linux/서버 환경 cron 설정 가이드 작성
