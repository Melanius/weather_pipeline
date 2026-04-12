# llm_for_ship — 환경 데이터 파이프라인 개발 기록

> **프로젝트**: 한화오션 × 동국대 산학협력 — 선박 특화 LLM 개발 (2026년 과제)
> **담당자**: 이훈정 책임 (한화오션)
> **담당 범위**: 환경 데이터 수집 → PostgreSQL(TimescaleDB) 적재 파이프라인
> **작업 환경**: Windows 11 + WSL2 (Ubuntu) / Python 3.12 / UV / Docker Desktop
> **최초 작성일**: 2026-03-15 (KST) | **최종 수정일**: 2026-04-13 (KST, Phase 17 다운로드/적재 상태 분리 + 자동 재시도 완료)

---

## 목차
1. [프로젝트 목적](#1-프로젝트-목적)
2. [전체 데이터 흐름](#2-전체-데이터-흐름)
3. [개발 환경 설정](#3-개발-환경-설정)
4. [폴더 구조](#4-폴더-구조)
5. [완료된 개발 내역](#5-완료된-개발-내역)
6. [설정 파일 상세](#6-설정-파일-상세)
7. [DB 스키마](#7-db-스키마)
8. [실행 방법](#8-실행-방법)
9. [미완료 작업 (TODO) — 다음 세션 시작점](#9-미완료-작업-todo--다음-세션-시작점)
10. [주요 기술 결정 사항](#10-주요-기술-결정-사항)
11. [알려진 제약사항 및 주의사항](#11-알려진-제약사항-및-주의사항)
12. [GitHub 저장소](#12-github-저장소)

---

## 1. 프로젝트 목적

선박 특화 LLM이 항로 계획, 안전 운항 지원 등의 질의응답을 수행할 수 있도록
**과거·현재·미래 환경 데이터(기상·해양)를 주기적으로 수집하여 TimescaleDB에 적재**하는 파이프라인 구축.

### 1-1. 수집 데이터 종류 — 재분석(과거) ✅ 완료

| 데이터 | 출처 | 변수 | 해상도 | 시간 간격 |
|--------|------|------|--------|----------|
| 바람 (Wind) | ECMWF ERA5/ERA5T (CDS API) | u10, v10 | 0.25° | 1시간 |
| 파랑 (Wave) | ECMWF ERA5/ERA5T (CDS API) | swh, mwd, mwp, shts, mdts, mpts, shww, mdww, mpww | 0.25° | 1시간 |
| 해류 (Current) | HYCOM 분석 OPeNDAP | water_u, water_v | ~0.24° (stride=3) | 3시간 |

### 1-2. 수집 데이터 종류 — 예보(미래) ✅ 완료 (개발 + DB 적재 테스트 완료)

| 데이터 | 출처 | 변수 | 해상도 | 예보 기간 |
|--------|------|------|--------|----------|
| 바람 예보 | ECMWF Open Data (ecmwf-opendata 라이브러리) | u10, v10 | 0.25° | 10일 |
| ~~파랑 예보~~ | ~~ECMWF Open Data~~ | ~~swh, mwd, mwp 3개만 (너울·풍파 6개 미제공)~~ | ~~0.25°~~ | ~~10일~~ |
| ↑ **수집 중단** (2026-03-17) | NOAA WW3로 통합 — ECMWF 무료 tier 6개 변수 미제공 확인 | | | |
| 파랑 예보 | **NOAA WaveWatch III (PacIOOS ERDDAP)** | swh, mwd, mwp, shts, mdts, mpts, shww, mdww, mpww **9개 완전** | 0.5° | 5일 |
| 해류 예보 | HYCOM Forecast OPeNDAP | water_u, water_v | ~0.24° | 5일 |

---

## 2. 전체 데이터 흐름

### 2-1. 현재 구현된 흐름 (재분석)

```
[입력]  config/down.json
         └─ { "start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD" }

[수집 1] ECMWF ERA5/ERA5T (CDS API)
         ├─ Wind: u10, v10
         └─ Wave: swh, mwd, mwp, shts, mdts, mpts, shww, mdww, mpww
         └─ 저장: data/ecmwf/reanalysis/YYYY/MM/ecmwf_wind_YYYYMMDD.nc
                                              ecmwf_wave_YYYYMMDD.nc

[수집 2] HYCOM 분석 (OPeNDAP 스트리밍, API 키 불필요)
         └─ Current: water_u, water_v (수심 0m, stride=3)
         └─ 저장: data/hycom/current/YYYY/MM/hycom_current_YYYYMMDD.nc

[적재]   NetCDF → DataFrame → PostgreSQL COPY 명령
         ├─ ecmwf_wind/wave_* → env_ecmwf_reanalysis 테이블
         └─ hycom_current_*   → env_hycom_current 테이블
```

### 2-2. 목표 전체 흐름 (재분석 + 공백처리 + 예보)

```
타임라인:
  과거(2021~)   D-5       D-3       D-1    오늘(D)         D+5      D+10
  ────────────  ──────────────────────────  ──────────────────────────────
  [ERA5 재분석]  [ERA5T]              [HYCOM 분석]
                          ↑ 공백 구간 ↑     [ECMWF 예보(바람) 10일         ]
                                            [NOAA WW3 예보(파랑 9개) 5일   ]
                                            [HYCOM 예보(해류) 5일           ]
  ※ ECMWF 예보 파랑은 2026-03-17 수집 중단 → NOAA WW3로 통합

공백 처리 전략 (B안+C안 조합):
  ① ERA5T : D-3까지 재분석 수준 데이터 (CDS API, 기존 코드 파라미터만 추가)
  ② 예보 누적: 매일 예보를 저장 → 5일 후 이전 예보가 공백 구간을 자동 커버
  → 파이프라인 가동 후 5일이 지나면 공백 구조적으로 해소

데이터 우선순위 (동일 시각·위치 데이터가 중복될 경우):
  ERA5(최종확정) > ERA5T(임시) > ECMWF 예보
  ※ ERA5T와 ERA5의 품질 차이는 실용상 무시 가능 수준
  ※ 1차 개발에서는 ON CONFLICT DO NOTHING (먼저 적재된 것 유지)
  ※ 추후 필요시 UPSERT로 변경 가능
```

---

## 3. 개발 환경 설정

### 3-1. 기본 환경
- OS: Windows 11 + WSL2 (Ubuntu)
- Python: 3.12
- 패키지 관리: **UV** (`uv sync` 로 의존성 설치)
- 컨테이너: Docker Desktop (Windows)

### 3-2. CDS API 인증 정보 (`.env` 파일)
```
CDS_API_URL=https://cds.climate.copernicus.eu/api
CDS_API_KEY=a6e4a459-f5d9-4073-85c0-8c728bb2a592
```
> ⚠️ `.env` 파일은 `.gitignore`에 포함되어야 함 (보안)

### 3-3. DB 접속 정보 (`.env` 파일)
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ship_env
DB_USER=shipllm
DB_PASSWORD=shipllm1234
```

### 3-4. Docker (TimescaleDB) 실행
```bash
docker compose up -d    # 백그라운드 기동
docker compose ps       # 상태 확인
docker compose down     # 종료
```

### 3-5. UV 환경 설정
```bash
# WSL (개발 환경)
cd /mnt/c/Users/hjlee/llm_for_ship
uv sync    # 최초 1회 또는 pyproject.toml 변경 후

# Windows (회사 로컬 PC 배포 환경)
cd C:\Users\hjlee\llm_for_ship
uv sync    # Windows x64 지원 추가됨 (2026-03-18)
```

> **Windows 지원 (2026-03-18 추가)**: `pyproject.toml`의 `[tool.uv] environments`에 `sys_platform == 'win32'` 추가.
> 이전에는 Linux 전용으로 잠금되어 Windows에서 `uv sync` 실패했음.
> ※ `eccodes`/`cfgrib` (ECMWF 예보 GRIB2 처리) Windows 동작 여부는 회사 PC 셋팅 시 확인 필요.
>   실패 시: `conda install -c conda-forge eccodes cfgrib` 으로 대체 설치.

### 3-6. ~~다음 개발에서 추가될 라이브러리~~ (완료)
```
ecmwf-opendata   # ✅ 이미 추가됨 (Phase 6)
cfgrib           # ✅ 이미 추가됨 (Phase 6)
eccodes          # ✅ 이미 추가됨 (Phase 6)
```

---

## 4. 폴더 구조

### 4-1. 현재 구조 (✅ Phase 8 완료)
```
llm_for_ship/
├── config/
│   ├── settings.toml          # 파이프라인 설정 (noaa_forecast 섹션 포함)
│   └── down.json              # 재분석 다운로드 날짜 범위
│
├── data/                      # 자동 생성 (gitignore)
│   ├── ecmwf/
│   │   ├── reanalysis/YYYY/MM/
│   │   │   ├── ecmwf_wind_YYYYMMDD.nc
│   │   │   └── ecmwf_wave_YYYYMMDD.nc
│   │   └── forecast/YYYY/MM/
│   │       └── ecmwf_fc_wind_YYYYMMDD.nc   (u10/v10 — 바람만)
│   │           ※ ecmwf_fc_wave_*.nc 수집 중단 (2026-03-17, NOAA WW3로 통합)
│   ├── hycom/
│   │   ├── current/YYYY/MM/
│   │   │   └── hycom_current_YYYYMMDD.nc
│   │   └── forecast/YYYY/MM/
│   │       └── hycom_fc_current_YYYYMMDD.nc
│   └── noaa/                  # ✅ 신규 (Phase 8)
│       └── forecast/YYYY/MM/
│           └── noaa_fc_wave_YYYYMMDD.nc    (파랑 9개 변수 완전)
│
├── docs/
│   └── Project_Plan.md
│
├── scripts/
│   ├── check_forecast_vars.py # ✅ NOAA 폴더 탐색 추가
│   └── test_noaa_erddap.py    # ✅ 신규 (Phase 8) — ERDDAP URL 접근 테스트
│
├── specs/
│   ├── ecmwf_reanalysis.md
│   ├── ecmwf_forecast.md
│   ├── hycom_forecast.md
│   └── db_schema.md
│
├── src/env_pipeline/
│   ├── pipeline.py            # ✅ NOAA WW3 다운로더 통합 (Phase 8)
│   ├── ecmwf/
│   │   ├── era5_downloader.py
│   │   └── ecmwf_forecast_downloader.py
│   ├── hycom/
│   │   ├── hycom_downloader.py
│   │   └── hycom_forecast_downloader.py
│   ├── noaa/                  # ✅ 신규 (Phase 8)
│   │   ├── __init__.py
│   │   └── noaa_forecast_downloader.py
│   └── db/
│       ├── connection.py
│       ├── schema.py          # ✅ env_noaa_forecast 테이블 추가 (13단계)
│       └── loader.py          # ✅ noaa_fc_wave 파일 라우팅 추가
│
├── run.py
├── pyproject.toml
├── docker-compose.yml
└── .env
```

---

## 5. 완료된 개발 내역

### Phase 1 — 프로젝트 기반 설정 (2026-03-15 KST)
- [x] 프로젝트 폴더 구조 설계 및 생성
- [x] `pyproject.toml` 작성 (UV 기반, 의존성 정의)
- [x] `docker-compose.yml` 작성 (TimescaleDB)
- [x] `.env` 파일 작성 (CDS API 키, DB 접속 정보)
- [x] `config/settings.toml` 작성 (ECMWF 변수 목록, 해상도 등)
- [x] `config/down.json` 작성 (날짜 범위 입력 방식)
- [x] Spec 문서: `specs/ecmwf_reanalysis.md`, `specs/db_schema.md`

### Phase 2 — ECMWF ERA5 재분석 다운로더 (2026-03-15 KST)
- [x] `src/env_pipeline/ecmwf/era5_downloader.py`
  - `load_date_range(json_path)`: down.json 읽기
  - `ERA5Downloader.download_day(date, data_type, resolution)`: 1일치 wind/wave 다운로드
  - `ERA5Downloader.run(json_path, resolution)`: 날짜 범위 전체 순차 다운로드
  - 저장: `data/ecmwf/reanalysis/YYYY/MM/ecmwf_{wind|wave}_YYYYMMDD.nc`
  - 파일 이미 있으면 건너뜀 (중복 방지)

### Phase 3 — DB 스키마 및 적재 모듈 (2026-03-15 KST)
- [x] `src/env_pipeline/db/connection.py`: `.env` 기반 DB 연결
- [x] `src/env_pipeline/db/schema.py`
  - `env_ecmwf_reanalysis` 테이블, Hypertable, 인덱스, 압축 정책
  - `env_hycom_current` 테이블, Hypertable, 압축 정책
- [x] `src/env_pipeline/db/loader.py`
  - `_detect_table_config(nc_path)`: 파일명 → 테이블/컬럼 자동 감지
  - `load_netcdf_to_db()`: NetCDF → COPY 적재 (배치)
  - `load_multiple_files()`: 다중 파일 순차 적재

### Phase 4 — HYCOM 해류 분석 다운로더 (2026-03-15 KST)
- [x] `src/env_pipeline/hycom/hycom_downloader.py`
  - OPeNDAP URL: `https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0/uv3z#fillmismatch`
  - `isel(depth=0)`: 수심 0m 해수면만
  - `isel(lat/lon stride=3)`: 0.24° 해상도
  - 경도 변환: 0~360 → -180~180
  - 저장: `data/hycom/current/YYYY/MM/hycom_current_YYYYMMDD.nc`

### Phase 5 — 파이프라인 통합 (2026-03-15 KST)
- [x] `src/env_pipeline/pipeline.py`: ECMWF + HYCOM 통합 오케스트레이터 (3가지 mode)
- [x] `run.py`: CLI 진입점 (`--mode`, `--init-db`)
- [x] `config/settings.toml`: `[hycom]` stride=3, `hycom_current_dir` 추가

### Phase 6 — 예보 파이프라인 구현 (2026-03-15 KST) ✅ 실행 테스트 완료 (2026-03-16)
- [x] `specs/ecmwf_forecast.md`: ECMWF HRES 예보 다운로더 명세
- [x] `specs/hycom_forecast.md`: HYCOM 예보 해류 다운로더 명세
- [x] `pyproject.toml`: `ecmwf-opendata`, `cfgrib`, `eccodes` 의존성 추가
- [x] `src/env_pipeline/ecmwf/ecmwf_forecast_downloader.py`
  - `ecmwf-opendata` 라이브러리 → GRIB2 다운로드 → `cfgrib`으로 xarray 변환
  - 바람(u10/v10) 다운로드 후 NetCDF 저장 (**파랑은 Phase 9에서 제거**)
  - `issued_at` → `ds.attrs["issued_at"]` 에 저장
  - 저장: `data/ecmwf/forecast/YYYY/MM/ecmwf_fc_wind_YYYYMMDD.nc`
- [x] `src/env_pipeline/hycom/hycom_forecast_downloader.py`
  - HYCOM FMRC Best OPeNDAP URL: `FMRC_ESPC-D-V02_uv3z_best.ncd#fillmismatch`
  - 분석 다운로더와 동일한 방식, URL/시간범위만 다름
  - 저장: `data/hycom/forecast/YYYY/MM/hycom_fc_current_YYYYMMDD.nc`
- [x] `src/env_pipeline/db/schema.py`: `env_ecmwf_forecast`, `env_hycom_forecast` 테이블 추가 (11단계)
- [x] `src/env_pipeline/db/loader.py`: 예보 파일 패턴 감지 + `issued_at` 컬럼 처리 추가
- [x] `src/env_pipeline/pipeline.py`: 6가지 mode 지원 (`forecast`, `forecast_download_only`, `full_with_forecast` 추가)
- [x] `run.py`: argparse choices에 예보 3가지 mode 추가
- [x] `config/settings.toml`: `[ecmwf_forecast]`, `[hycom_forecast]` 섹션 + 예보 경로 추가

### Phase 7 — 실행 테스트 및 버그 수정 (2026-03-16 KST)

**실행 테스트로 확인된 실제 동작:**
- [x] ECMWF 예보 wave: Open Data 무료 tier에서 **기본 3개(swh, mwd, mwp)만 제공** (나머지 6개 미제공 확인)
  - fallback 동작 정상 확인 (9개 시도 → 실패 → 3개 재시도 → 성공)
  - DB의 shts, mdts, mpts, shww, mdww, mpww 컬럼은 NULL로 적재됨 (설계 의도대로)
- [x] ECMWF 예보 wind: u10, v10 정상 제공 (NaN 0%)
- [x] HYCOM 예보: water_u, water_v 정상, 경도 -180~180 정상, issued_at 저장 확인
- [x] ECMWF 재분석: 경도 0~360으로 저장되는 버그 발견 → `era5_downloader.py`에 경도 변환 추가
- [x] HYCOM 분석: time=0 빈 파일 생성 버그 발견 → `hycom_downloader.py`에 빈 파일 감지/재시도 추가
- [x] 테스트 도구 추가: `--forecast-days N` CLI 옵션 + `scripts/check_forecast_vars.py`

**버그 수정 내역:**
- `era5_downloader.py`: 다운로드 후 경도 0~360 → -180~180 변환 추가 (`_fix_longitude` 메서드)
- `hycom_downloader.py`: time=0 감지 시 파일 저장 안 함 + 기존 빈 파일 자동 삭제 후 재시도

**알려진 제약사항 (2026-03-16 확인):**
- ECMWF Open Data wave: `shts, mdts, mpts, shww, mdww, mpww` 6개 변수 **미제공** (무료 tier 제한)
  → NOAA WW3(Phase 8)로 보완 결정
- ERA5 재분석 경계 날짜: 오늘 기준 -5~-6일은 ERA5/ERA5T 혼재로 일부 시간 스텝 누락 가능
  → `down.json`의 `end_date`는 오늘 기준 -7일 이전으로 설정 권장
- **HYCOM expt_93.0 서비스 종료**: 2024-09-05에 데이터 종료 확인 → URL 변경 필요
  → `hycom_downloader.py` URL을 FMRC ESPC-D-V02 Best로 교체 완료 (2026-03-16)
  → FMRC Best는 최근 약 10일치만 보유: 오래된 과거 데이터 수집 시 별도 아카이브 URL 조사 필요

---

### Phase 8 — NOAA WaveWatch III 파랑 예보 파이프라인 (2026-03-16 KST)

**배경:**
- Phase 7에서 ECMWF Open Data가 너울·풍파 분리 변수 6개를 무료 tier에서 미제공함을 확인
- 이전 개발자(code/ecmwf_down.py)도 ERA5 재분석에서 파랑 3개만 수집 → 동일 제한 인지하고 있었음
- 파랑 9개 변수 완전 수집을 위해 NOAA WaveWatch III(PacIOOS ERDDAP) 추가

**구현 내용:**

- [x] `scripts/test_noaa_erddap.py`: NOAA ERDDAP URL 접근 테스트 스크립트
  - CoastWatch(coastwatch.pfeg.noaa.gov): HTTP OK이지만 OPeNDAP 프로토콜 불안정 (DAP malformed response)
  - **PacIOOS(pae-paha.pacioos.hawaii.edu)**: OPeNDAP 정상, 9개 변수 완전 제공 → 채택

- [x] `src/env_pipeline/noaa/__init__.py`: noaa 모듈 초기화 파일

- [x] `src/env_pipeline/noaa/noaa_forecast_downloader.py`: NOAA WW3 다운로더
  - URL: `https://pae-paha.pacioos.hawaii.edu/erddap/griddap/ww3_global` (OPeNDAP)
  - 변수 이름 변환: `Thgt→swh, Tdir→mwd, Tper→mwp, shgt→shts, sdir→mdts, sper→mpts, whgt→shww, wdir→mdww, wper→mpww`
  - 경도 변환: 0~360 → -180~180
  - xarray timedelta64 버그 수정: `sper`/`wper` 변수의 `units="seconds"`로 인해 xarray가 timedelta64로 잘못 해석
    → `(var / np.timedelta64(1, "s")).astype("float32")` 변환 후 `units="wave_seconds"` 로 저장
    → 파일 읽기 시 `decode_timedelta=False` 적용 (loader.py, check_forecast_vars.py 동일)
  - 저장: `data/noaa/forecast/YYYY/MM/noaa_fc_wave_YYYYMMDD.nc`
  - 최대 예보 기간: 7일 (PacIOOS WW3 제공 범위)

- [x] `src/env_pipeline/db/schema.py`: `env_noaa_forecast` 테이블 추가 (총 13단계, 5개 테이블)
  ```sql
  CREATE TABLE env_noaa_forecast (
      issued_at TIMESTAMPTZ NOT NULL,
      datetime  TIMESTAMPTZ NOT NULL,
      lat       REAL NOT NULL,
      lon       REAL NOT NULL,
      swh REAL, mwd REAL, mwp REAL,
      shts REAL, mdts REAL, mpts REAL,
      shww REAL, mdww REAL, mpww REAL,
      PRIMARY KEY (issued_at, datetime, lat, lon)
  );
  ```

- [x] `src/env_pipeline/db/loader.py`: `noaa_fc_wave` 파일 → `env_noaa_forecast` 라우팅 추가
  - NOAA_FC_WAVE_COLUMNS: issued_at, datetime, lat, lon + 9개 파랑 변수
  - `decode_timedelta=False` 적용 (NOAA 파일 읽기 시 timedelta 재해석 방지)

- [x] `src/env_pipeline/pipeline.py`: NOAA WW3 다운로더 통합
  - `noaa_fc_dir` 경로, `noaa_fc_days` 설정 추가
  - 예보 모드에서 HYCOM 이후 NOAA WW3 다운로드 실행

- [x] `config/settings.toml`:
  ```toml
  [noaa_forecast]
  forecast_days = 5

  [paths]
  noaa_forecast_dir = "data/noaa/forecast"
  ```

- [x] `scripts/check_forecast_vars.py`: NOAA forecast 폴더 탐색 + `noaa_fc_wave` 파일 패턴 추가

**다운로드 검증 결과 (2026-03-16):**

| 변수 | 제공여부 | NaN 비율 | 값 범위 |
|------|---------|---------|---------|
| swh | ✅ float32 | 35.8% | 0 ~ 13.76 m |
| mwd | ✅ float32 | 35.8% | 0 ~ 360° |
| mwp | ✅ float32 | 35.8% | 1.09 ~ 25.0 s |
| shts | ✅ float32 | 39.0% | 0.05 ~ 11.4 m |
| mdts | ✅ float32 | 39.0% | 0 ~ 360° |
| mpts | ✅ float32 | 39.0% | 1.49 ~ 27.49 s ← timedelta 버그 수정 확인 |
| shww | ✅ float32 | 66.0% | 0.05 ~ 13.7 m |
| mdww | ✅ float32 | 66.0% | 0 ~ 360° |
| mpww | ✅ float32 | 66.0% | 1.09 ~ 21.82 s ← timedelta 버그 수정 확인 |

- 파일: `noaa_fc_wave_20260316.nc` (369.1 MB, 48개 시간 스텝, 1시간 간격)
- 공간 범위: 위도 -77.5°~77.5°, 경도 -180°~180°, 해상도 0.5°

**GitHub Push:**
- [x] `https://github.com/Melanius/weather_pipeline` — 초기 커밋 (36개 파일, 6,721줄)

---

### Phase 9 — ECMWF 예보 파랑 수집 완전 제거 (2026-03-17 KST)

**배경:**
- Phase 7에서 ECMWF Open Data 파랑은 swh/mwd/mwp 3개만 무료 제공임을 확인
- Phase 8에서 NOAA WW3로 9개 변수 완전 수집 완료 및 검증
- 5일 예보로 충분하다는 판단 + 이중 수집 불필요 → ECMWF 파랑 수집 제거 결정

**제거 내용:**

- [x] `src/env_pipeline/ecmwf/ecmwf_forecast_downloader.py`
  - `WAVE_PARAMS_FULL`, `WAVE_PARAMS_BASIC` 상수 제거
  - `_download_wave()` 메서드 전체 제거 (~80줄)
  - `run()`: 바람 전용으로 단순화 (1/1 파일, "1/2, 2/2" 표시 제거)
  - docstring 갱신: 파랑은 NOAA WW3 전담 명시

- [x] `src/env_pipeline/db/schema.py`
  - `env_ecmwf_forecast` 테이블에서 파랑 9개 컬럼 완전 제거 (u10/v10만 유지)
  - 제거된 컬럼: swh, mwd, mwp, shts, mdts, mpts, shww, mdww, mpww

- [x] `src/env_pipeline/db/loader.py`
  - `ECMWF_FC_WAVE_COLUMNS` 상수 제거
  - `ecmwf_fc_wave` 파일 라우팅 브랜치 제거

- [x] `scripts/check_forecast_vars.py`
  - `FILE_PATTERNS`에서 `ecmwf_fc_wave` 패턴 제거
  - 주석 추가: "ecmwf_fc_wave는 수집 중단 (2026-03-17)"

- [x] `data/ecmwf/forecast/2026/03/ecmwf_fc_wave_20260315.nc` 삭제 (59.46 MB)
- [x] `data/ecmwf/forecast/2026/03/ecmwf_fc_wave_20260316.nc` 삭제 (59.46 MB)

**GitHub Push:**
- [x] commit `afc8d35` — "refactor: ECMWF 예보 파랑 수집 제거 — NOAA WW3로 전담"

---

### Phase 10 — DB 적재 첫 테스트 (2026-03-21 KST)

**배경:**
- Phase 9까지 코드 완성 후 최초로 실제 DB 적재 테스트 수행
- PC 재부팅 후 재시작 시 DB 적재 중 문제 발생하여 재시도

**수행 내용:**

- [x] Docker Desktop + TimescaleDB 컨테이너 기동 확인
- [x] `uv run python run.py --init-db`: 5개 테이블 정상 생성 확인
  - `env_ecmwf_reanalysis`, `env_hycom_current`
  - `env_ecmwf_forecast`, `env_hycom_forecast`, `env_noaa_forecast`
- [x] `down.json` 날짜 설정: `{"manual_start": "2026-03-05", "manual_end": "2026-03-12"}` (8일치)
- [x] 재분석 데이터 다운로드 확인 (2026-03-05~12 범위 wind/wave .nc 파일)
- [x] DB 적재 테스트 시도 → **적재 속도 심각하게 느림** 문제 발견
  - INSERT ON CONFLICT 방식: 24.9M행 기준 10분+ 예상 → Phase 11로 최적화 진행

---

### Phase 11 — DB 적재 성능 최적화 (2026-03-21 KST)

**문제:**
- 기존 방식(INSERT ON CONFLICT DO UPDATE)이 TimescaleDB hypertable에서 극도로 느림
- 단일 StringIO 버퍼에 24.9M행 전체 직렬화 → 메모리 과다 + 시간 과다
- PostgreSQL 세션이 프로세스 강제 종료 후에도 잠금 유지 (blocking session)

**최적화 내용:**

- [x] **세션 블로킹 해제**: 프로세스 강제 종료 후 남은 DB 세션 `pg_terminate_backend(pid)` 로 수동 종료
  - 방법: `docker exec llm_ship_timescaledb psql -U shipllm -d ship_env -c "SELECT pg_terminate_backend(<pid>);"`

- [x] **청크 스트리밍 COPY 방식 도입**: `COPY_CHUNK_SIZE = 500_000`
  - 24.9M행을 50개 청크(50만행 단위)로 분할하여 순차 COPY
  - 단일 버퍼(1.8GB+) 대신 청크별 소량 버퍼로 메모리 절약

- [x] **전략 A — 직접 COPY** (wind, hycom, 예보 파일 전용)
  - `COPY {target_table} ({col_str}) FROM STDIN WITH (FORMAT csv, NULL '\\N')`
  - 메인 테이블에 직접 청크 스트리밍 → 가장 빠름

- [x] **전략 B — COPY → UPDATE** (wave 파일 전용)
  - wind 파일이 먼저 적재(PK row 생성) 후 wave가 해당 행의 wave 컬럼만 채우는 구조
  - `INSERT ON CONFLICT DO UPDATE` 대신 `tmp_load 임시 테이블 → CREATE INDEX → UPDATE main` 순서
  - `SET LOCAL work_mem = '2GB'`: 24.9M 행 JOIN 해시 테이블이 디스크로 spill되는 것 방지
  - UPDATE SQL 버그 수정: SET 절에 테이블 alias 불가 (`r.col = t.col` → `col = t.col`)

**성능 측정 결과 (2026-03-05 1일치 기준):**

| 파일 유형 | 전략 | 총 소요 시간 | 행수 |
|-----------|------|------------|------|
| ecmwf_wind_20260305.nc | A (직접 COPY) | **10.7분** | 24,917,760 |
| ecmwf_wave_20260305.nc | B (COPY→UPDATE) | **21.9분** | 24,917,760 |

> COPY 청크 50개 ~8분 / tmp_load 인덱스 ~30초 / UPDATE 쿼리 ~11.6분

**현재 적재된 데이터 현황 (2026-03-21 기준):**

| 항목 | 값 |
|------|----|
| 적재 날짜 | 2026-03-05 1일치 |
| 총 행수 | 24,917,760 행 |
| 시간 범위 | 2026-03-05 00:00~23:00 UTC (24시간) |
| 격자 | 721 lat × 1440 lon (0.25°) |
| wind 컬럼 | 24,917,760행 (전체) |
| wave 컬럼 | 13,873,772행 (해양 55.7%) |
| DB 용량 | **7.2 GB** (1일치, 비압축) |

---

## 6. 설정 파일 상세

### config/settings.toml (현재)
```toml
[ecmwf]
dataset = "reanalysis-era5-single-levels"
product_type = "reanalysis"
data_format = "netcdf"
spatial_resolution = 0.25

[ecmwf.variables]
wave = [9개 변수: swh, mwd, mwp, shts, mdts, mpts, shww, mdww, mpww]
wind = [2개 변수: u10, v10]

[hycom]
stride = 3   # 0.08° × 3 = 0.24°

[database]
table_reanalysis = "env_ecmwf_reanalysis"
chunk_time_interval = "7 days"
batch_size = 50000

[paths]
ecmwf_reanalysis_dir = "data/ecmwf/reanalysis"
hycom_current_dir    = "data/hycom/current"
log_dir = "logs"
```

### config/settings.toml (예보 추가 — ✅ 완료)
```toml
# 위 내용 유지하고 아래 추가됨
[ecmwf_forecast]
forecast_days = 10        # 예보 기간 (일, 최대 240h)
step_hours = 6            # 예보 간격 (시간)

[hycom_forecast]
forecast_days = 5         # HYCOM 예보 기간 (일, 최대 5일)

[noaa_forecast]
forecast_days = 5         # NOAA WW3 예보 기간 (일, PacIOOS 최대 ~7일)

[paths]
# 기존 유지하고 아래 추가됨
ecmwf_forecast_dir   = "data/ecmwf/forecast"
hycom_forecast_dir   = "data/hycom/forecast"
noaa_forecast_dir    = "data/noaa/forecast"
```

### config/down.json 형식
```json
{ "start_date": "2026-03-10", "end_date": "2026-03-10" }
```
> ⚠️ ERA5 5일 지연: 오늘(2026-03-15) 기준 end_date ≤ 2026-03-10 만 가능

---

## 7. DB 스키마

### 7-1. 현재 구현된 테이블 (재분석)

```sql
-- ECMWF 바람 + 파랑 통합 (wind 파일과 wave 파일이 같은 테이블에 적재)
CREATE TABLE env_ecmwf_reanalysis (
    datetime  TIMESTAMPTZ NOT NULL,
    lat       REAL        NOT NULL,
    lon       REAL        NOT NULL,
    swh REAL, mwd REAL, mwp REAL,       -- Wave (wave 파일에서 채워짐)
    shts REAL, mdts REAL, mpts REAL,
    shww REAL, mdww REAL, mpww REAL,
    u10 REAL, v10 REAL,                 -- Wind (wind 파일에서 채워짐)
    PRIMARY KEY (datetime, lat, lon)
);
-- Hypertable: 7일 청크 / 30일 후 자동 압축

-- HYCOM 해류
CREATE TABLE env_hycom_current (
    datetime  TIMESTAMPTZ NOT NULL,
    lat       REAL        NOT NULL,
    lon       REAL        NOT NULL,
    water_u   REAL,
    water_v   REAL,
    PRIMARY KEY (datetime, lat, lon)
);
-- Hypertable: 7일 청크 / 30일 후 자동 압축
```

### 7-2. 추가된 테이블 (예보) ✅ 완료

```sql
-- ECMWF 예보 (바람만 — ✅ Phase 9에서 파랑 컬럼 완전 제거 2026-03-17)
-- ※ ECMWF Open Data 무료 tier 파랑 미제공 확인 → NOAA WW3(env_noaa_forecast)로 통합
CREATE TABLE env_ecmwf_forecast (
    issued_at TIMESTAMPTZ NOT NULL,
    datetime  TIMESTAMPTZ NOT NULL,
    lat       REAL        NOT NULL,
    lon       REAL        NOT NULL,
    u10 REAL, v10 REAL,                 -- 10m 동서/남북 풍속
    PRIMARY KEY (issued_at, datetime, lat, lon)
);

-- HYCOM 예보 (해류)
CREATE TABLE env_hycom_forecast (
    issued_at TIMESTAMPTZ NOT NULL,
    datetime  TIMESTAMPTZ NOT NULL,
    lat       REAL        NOT NULL,
    lon       REAL        NOT NULL,
    water_u   REAL,
    water_v   REAL,
    PRIMARY KEY (issued_at, datetime, lat, lon)
);

-- NOAA WaveWatch III 예보 (파랑 9개 완전) ✅ Phase 8 신규
-- ECMWF에서 미제공하는 너울/풍파 분리 변수 포함
CREATE TABLE env_noaa_forecast (
    issued_at TIMESTAMPTZ NOT NULL,
    datetime  TIMESTAMPTZ NOT NULL,
    lat       REAL        NOT NULL,
    lon       REAL        NOT NULL,
    swh REAL, mwd REAL, mwp REAL,
    shts REAL, mdts REAL, mpts REAL,    -- 너울(Swell) 유의파고/방향/주기
    shww REAL, mdww REAL, mpww REAL,    -- 풍파(Wind Wave) 유의파고/방향/주기
    PRIMARY KEY (issued_at, datetime, lat, lon)
);
```

> **issued_at 컬럼이 필요한 이유**: 같은 미래 시각(예: 3일 후 12:00)의 예보가 매일 갱신되므로
> 어떤 날 발행된 예보인지 구분해야 함. LLM Agent는 최신 issued_at 기준으로 조회.

> **ECMWF wave 컬럼 제거 완료 (2026-03-17)**: `env_ecmwf_forecast`는 u10/v10 바람 전용. 파랑 9개 변수는 `env_noaa_forecast` 참조.

---

## 8. 실행 방법

### 8-1. 현재 (재분석 전용)
```bash
docker compose up -d                          # TimescaleDB 기동
uv run python run.py --init-db                # DB 스키마 초기화 (최초 1회)
uv run python run.py --mode download_only     # 다운로드만 (테스트용)
uv run python run.py --mode load_only         # DB 적재만
uv run python run.py --mode full              # 다운로드 + 적재
```

### 8-2. 예보 파이프라인 ✅ 완료
```bash
# 예보 다운로드만 (ECMWF + HYCOM + NOAA WW3)
uv run python run.py --mode forecast_download_only

# 빠른 테스트 (1일치만)
uv run python run.py --mode forecast_download_only --forecast-days 1

# 예보 다운로드 + DB 적재
uv run python run.py --mode forecast

# 재분석 + 예보 동시 실행
uv run python run.py --mode full_with_forecast
```

### 8-3. 파일 검증 도구
```bash
# 다운로드된 모든 예보 파일 변수 구조 확인
uv run python scripts/check_forecast_vars.py

# 특정 파일만 확인
uv run python scripts/check_forecast_vars.py --file data/noaa/forecast/2026/03/noaa_fc_wave_20260316.nc
```

---

## 9. 미완료 작업 (TODO) — 다음 세션 시작점

### ✅ 완료된 즉시 작업 (Phase 7~8, 2026-03-16)

#### Step 1. 재분석 파이프라인 실행 테스트
- [x] 의존성 설치 확인 완료
- [x] ECMWF ERA5 재분석 다운로드 확인 (2026-03-09, 2026-03-10)
- [x] HYCOM 분석 다운로드 시도 → time=0 버그 발견 및 수정 완료

#### Step 2. 예보 파이프라인 실행 테스트
- [x] `ecmwf_fc_wind_YYYYMMDD.nc` 생성 확인 (u10, v10 정상)
- [x] `ecmwf_fc_wave_YYYYMMDD.nc` 생성 확인 (swh, mwd, mwp 3개 — 무료 tier 제한 확인)
- [x] `hycom_fc_current_YYYYMMDD.nc` 생성 확인 (water_u, water_v 정상)
- [x] fallback 동작 확인 (9개 시도 → 3개 fallback)

#### Step 3. NOAA WW3 파이프라인 구현 (Phase 8)
- [x] PacIOOS ERDDAP URL 접근 테스트 → 정상 확인
- [x] `NOAAForecastDownloader` 구현 + xarray timedelta 버그 해결
- [x] `env_noaa_forecast` DB 테이블 설계 및 스키마 추가
- [x] loader.py 라우팅 추가
- [x] pipeline.py 통합
- [x] `noaa_fc_wave_20260316.nc` 다운로드 및 9개 변수 검증 완료
- [x] GitHub `https://github.com/Melanius/weather_pipeline` 초기 Push

#### Step 4. ECMWF 예보 파랑 수집 제거 (Phase 9, 2026-03-17)
- [x] NOAA WW3 9개 변수 완전 제공 검증 완료 → ECMWF 파랑 수집 불필요 판단
- [x] `ecmwf_forecast_downloader.py`에서 `_download_wave()` 제거
- [x] `schema.py` `env_ecmwf_forecast` 파랑 9개 컬럼 제거 (u10/v10만 유지)
- [x] `loader.py` `ECMWF_FC_WAVE_COLUMNS` 및 라우팅 제거
- [x] `check_forecast_vars.py` `ecmwf_fc_wave` 패턴 제거
- [x] 기존 `ecmwf_fc_wave_*.nc` 파일 2개 삭제 (118.9 MB 회수)
- [x] commit `afc8d35` push 완료

### ✅ 완료된 즉시 작업 (Phase 10~11, 2026-03-21)

#### Step 5. DB 적재 테스트 및 최적화 (Phase 10~11)
- [x] Docker Desktop 기동 → `uv run python run.py --init-db` (5개 테이블 생성 확인)
- [x] 재분석 데이터 다운로드 확인 (2026-03-05~12)
- [x] DB 적재 성능 최적화 (Phase 11)
  - Strategy A (직접 COPY): wind 10.7분 ✅
  - Strategy B (COPY→UPDATE, work_mem=2GB): wave 21.9분 ✅
- [x] 2026-03-05 1일치 wind+wave 적재 완료 (24,917,760행, 7.2GB)

### ✅ 완료된 즉시 작업 (Phase 12, 2026-04-04)

#### Phase 10 재확인 (코드 선행 구현 확인)
- [x] `db/coverage.py`, `schema.py`, `run.py`, `pipeline.py` 모두 이전 세션에 선행 구현 완료 확인
  - pipeline_coverage 테이블 / `--diagnose` / `--dry-run` / `--manual` / `--simulate-date` 전부 완료

#### Phase 12 — Wind 해양 격자 필터링 (2026-04-04)

**배경:**
- ERA5 Wind(u10/v10)는 전 지구 격자(육지+바다) 모두에 값이 존재 → 24.9M행 전체 적재됨
- 선박은 바다에서만 운항 → 육지 격자 불필요 → DB 용량 약 44% 절감 가능
- Wave(swh 등)는 육지 격자에 NaN → 해양 마스크로 활용 가능

**구현 내용:**
- [x] `src/env_pipeline/db/loader.py`에 **단계 3.6** 추가 (`ecmwf_wind` 전용 해양 필터링)
  - 같은 날짜의 `ecmwf_wave_YYYYMMDD.nc`에서 `swh` 변수의 NaN 아닌 격자 = 해양으로 판단
  - wave 파일 없으면 경고 출력 후 필터링 건너뜀 (안전 장치)
  - float32 타입 통일 후 inner join으로 해양 격자만 필터링
- [x] `scripts/test_wind_ocean_filter.py` 신규 (DB 없이 필터링 로직만 검증)

**테스트 결과 (2026-03-05 기준):**
```
필터링 전: 24,917,760행 (전 지구)
필터링 후: 13,883,064행 (해양만)
제거된 행: 11,034,696행 (육지, 44.3%)
u10/v10 NaN: 0건 ✅
해양 비율: 55.7% (wave 파일 기준과 일치)
```

**결정 사항:**
- 2026-03-05 기존 적재 데이터(24.9M행, 육지 포함)는 **그대로 유지** (재적재 안 함)
- 2026-03-06 이후 신규 적재분부터 해양 필터링 적용

### ✅ 완료된 즉시 작업 (Phase 13, 2026-04-04)

#### Step 6. HYCOM 해류 DB 적재 테스트 ✅

- [x] Docker Desktop + TimescaleDB 컨테이너 기동 확인
- [x] `config/down.json` → `manual_start/manual_end = "2026-03-30"` 설정
- [x] `--mode download_only --manual`: `hycom_current_20260330.nc` 다운로드 완료 (129.8 MB, 8스텝)
  - ECMWF `ecmwf_wind_20260330.nc` (4.0 MB), `ecmwf_wave_20260330.nc` (8.7 MB)도 함께 다운로드됨
- [x] **`load_hycom_only` 모드 신규 추가** (Phase 13): HYCOM 해류만 단독 DB 적재
  - `pipeline.py`: `load_hycom_only` 분기 추가 (ecmwf_dates=[], hycom_dates만 처리)
  - `run.py`: `--mode choices`에 `"load_hycom_only"` 추가
- [x] `--mode load_hycom_only --manual`: `hycom_current_20260330.nc` DB 적재 완료
  - 소요: **6분** (11:20→11:26)
- [x] 적재 결과 확인:
  ```
  count=17,004,000 | MIN=2026-03-30 00:00 UTC | MAX=2026-03-30 21:00 UTC ✅
  ```

**신규 실행 명령:**
```bash
# HYCOM 해류만 단독 적재 (ECMWF 건너뜀)
uv run python run.py --mode load_hycom_only --manual
# --manual 없으면 data/hycom/current/ 전체 파일 스캔
uv run python run.py --mode load_hycom_only
```

### ✅ 완료된 작업 (Phase 14, 2026-04-05)

#### Phase 14-A: 완료 판정 기준 변경 ✅
- [x] `db/coverage.py`에서 `EXPECTED_ROWS` 행 수 비교 방식 제거
- [x] 에러 없음 = `complete`, 예외 발생 시 `failed` 판정으로 변경
- [x] `EXPECTED_ROWS` 상수 전체 삭제

#### Phase 14-B: 예보 테이블 PK 재설계 ✅
- [x] 3개 예보 테이블 PK: `(issued_at, datetime, lat, lon)` → `(datetime, lat, lon)`
- [x] `issued_at` 일반 컬럼으로 유지 (정보용)
- [x] `reinit_forecast_tables()` 함수 추가, `--reinit-forecast` 플래그 추가
- [x] `run.py --reinit-forecast`로 스키마 마이그레이션 실행 완료

#### Phase 14-C: 예보 pipeline_coverage 기록 추가 ✅
- [x] `pipeline.py`의 예보 파이프라인 완료 시 소스별 `forecast_only` 상태 기록
- [x] 예보 적재 실패 시 `failed` 기록

#### Phase 14-D: 예보 파이프라인 DB 적재 테스트 ✅ (2026-04-05)

**발견 및 해결한 버그 5종:**

| 버그 | 원인 | 해결 |
|------|------|------|
| off-by-one (날짜 범위) | `forecast_end`가 N+1일 자정 → 1일 초과 수집 | `(forecast_end - timedelta(seconds=1))` 적용 (HYCOM, NOAA 모두) |
| HYCOM OOM — 파일 크기 | float64 기본값 + 비압축 저장 → 1,046 MB | float32 변환 + zlib 압축(complevel=4) → 526 MB |
| HYCOM OOM — water_v 시간차원 | HYCOM FMRC에서 `water_u`는 `time`(8스텝), `water_v`는 `time1`(121스텝) 사용 → `sel(time=...)` 필터 미적용 | 변수별 시간차원 개별 탐색 후 선택, `inner merge`로 공통 타임스텝(3시간 간격)만 유지 |
| HYCOM OOM — DB 적재 | `ds.to_dataframe()` 전체 일괄 변환 시 OOM | `_load_by_timesteps()` 헬퍼 함수 신규 구현 — 시간 스텝 1개씩 처리 |
| ECMWF 예보 wind 육지 잔존 | UPSERT(전략 C)는 신규 데이터에 없는 행(육지)을 삭제 못 함 | 전략 D(TRUNCATE → COPY)로 교체 — 항상 최신 예보만 유지 |
| ECMWF 예보 wind 해양필터 미적용 | 예보 wind는 오늘 날짜 → 재분석 wave 파일(5~7일 지연) 없음 | `_find_latest_reanalysis_wave()` 추가 — 재분석 폴더에서 최신 wave 파일 자동 탐색 |

**최종 적재 결과:**
```
env_ecmwf_forecast:  2,856,870행  (2026-04-04, 5스텝 × 6시간 간격, 해양 필터 55%)
env_hycom_forecast: 17,004,000행  (2026-04-04, 8스텝 × 3시간 간격)
env_noaa_forecast:   3,428,472행  (2026-04-04, 24스텝 × 1시간 간격)
```

---

### ✅ 완료된 작업 (Phase 15-A, 2026-04-05)

#### Phase 15-A: 스케줄러 설정 (Windows Task Scheduler) ✅

**수집 시작일 제한 (용량 절감):**
- `config/settings.toml`에 `reanalysis_start_date = "2026-03-29"` 추가
- `coverage.py` `get_backfill_dates()`: `max(today - lookback_days, reanalysis_start_date)` 하한선 적용
- 효과: 2026-03-29 이전 날짜는 재분석 수집 대상에서 영구 제외

**생성 파일:**
- `scheduler/run_pipeline.bat` — WSL 경유 Python 실행 배치 파일
- `scheduler/pipeline_task.xml` — Task Scheduler 등록 XML

**Task Scheduler 등록 명령 (PowerShell 관리자 권한):**
```powershell
schtasks /Create /XML "C:\Users\hjlee\llm_for_ship\scheduler\pipeline_task.xml" /TN "llm_for_ship_pipeline"
```

**스케줄러 동작:**
- 매일 10:00 KST 자동 실행 (`--mode auto`)
- 실행 로그: `logs/pipeline_YYYY-MM-DD.log`
- 조건: 네트워크 연결 시만 실행
- 실패 시 1시간 후 1회 재시도
- PC가 꺼져 있다가 켜지면 즉시 실행 (`StartWhenAvailable=true`)

---

### ✅ 완료된 작업 (Phase 15-B, 2026-04-05)

#### Phase 15-B: 관리자 모니터링 대시보드 (Streamlit) ✅

**구현 파일:**
- `monitoring/app.py` — Streamlit 대시보드 메인 파일
- `.streamlit/config.toml` — 서버 설정 (이메일 프롬프트 비활성화)

**대시보드 구성 화면 (5개 섹션):**

| 섹션 | 내용 |
|------|------|
| **① 오늘 실행 요약** | 실행 시작 시각 / 실행 상태 / 예보 커버리지 / 누락 항목 카드 |
| **② 예보 커버리지 현황** | 예보 테이블별 실제 커버 일수 vs 목표 일수 |
| **③ 데이터 커버리지 달력** | 미래 10일(예보) + 과거 30일(재분석) × Wind/Wave/Current 3컬럼 |
| **④ DB 테이블 현황** | 테이블별 전체 행 수 + 날짜 범위 |
| **⑤ 최근 로그** | 날짜 선택 / 레벨 필터 / 줄 수 슬라이더 + 로그만 부분 새로고침 버튼 |

**커버리지 달력 컬럼 설계:**
- 컬럼: `Wind / Wave / Current` (재분석 + 예보 통합 표시)
- 재분석 완료 셀: `✅ HH:MM` (KST 기준 적재 완료 시각 표시)
- 재분석 + 예보 공존: `✅ HH:MM/🔵`
- 미래 날짜(예보만): `🔵`

**실행 방법:**
```bash
uv run streamlit run monitoring/app.py
# 브라우저: http://localhost:8501
```

**주요 버그 수정 이력:**
- `pd.read_sql_query` SQLAlchemy 경고 → psycopg2 cursor 직접 사용 (`_fetch_df()` 헬퍼)
- 첫 실행 이메일 프롬프트 → `.streamlit/config.toml` `gatherUsageStats = false`
- Wind/Wave 재분석 컬럼 누락 → base DataFrame 프리셋 후 merge로 해결

---

### ✅ 완료된 작업 (Phase 15-C, 2026-04-05)

#### Phase 15-C: 재분석 백필 (2026-03-29 ~ 2026-04-02) ✅

**배경:**
- `era5_delay_days = 7` 기본값으로는 오늘(4/5) 기준 3/29만 수집 가능
- 3/30~4/2 구간 수집을 위해 임시로 `era5_delay_days = 3`으로 변경 후 수동 실행

**1단계 (era5_delay_days=7):** 3/29 ECMWF + HYCOM 수집
**2단계 (era5_delay_days=3):** 3/30~3/31 ECMWF + 3/31~4/2 HYCOM 추가 수집

**최종 커버리지 결과:**
| 날짜 | ECMWF (Wind+Wave) | HYCOM (Current) |
|------|-------------------|-----------------|
| 3/29 | ✅ complete | ✅ complete |
| 3/30 | ✅ complete | ✅ complete |
| 3/31 | ✅ complete | ✅ complete |
| 4/01 | ❌ failed (ERA5T 미제공) | ✅ complete |
| 4/02 | ❌ failed (ERA5T 미제공) | ✅ complete |

- 4/1, 4/2 ECMWF: ERA5T 아직 미제공 → 수일 내 스케줄러 자동 재시도로 수집 예정
- 작업 후 `era5_delay_days = 7`로 복원 완료

**예보 데이터 (최신 issued_at 기준):**
```
env_ecmwf_forecast:  23,405,793행  (2026-04-05 issued, 10일 예보, 해양 필터 55%)
env_hycom_forecast:  85,020,000행  (2026-04-05 issued, 5일 예보)
env_noaa_forecast:   17,127,120행  (2026-04-05 issued, 5일 예보)
```

---

### ✅ 완료된 작업 (Phase 16, 2026-04-12)

#### Phase 16: 모니터링 대시보드 개선 ✅

**배경:**
- 파이프라인 실행 중 에러 로그가 있으면 무조건 "오류"로 표시 → 실제 진행 상황 파악 불가
- 로그 새로고침 시 전체 페이지 재로드 → 불편
- 커버리지 달력에 적재 완료 시각 미표시

**변경 내용 (`monitoring/app.py`):**

**① 실행 상태 판정 로직 전면 개선 (`get_last_run_info()`)**
- 기존: 오늘 로그 전체에 ERROR가 하나라도 있으면 "오류" 표시 (이전 실행 에러도 포함)
- 변경: 마지막 파이프라인 시작 이후 로그만 검사 → 에러 여부가 아닌 **진행 여부**로 판정

| 상태 | 표시 | 판정 기준 |
|------|------|----------|
| `success` | ✅ 완료 | "파이프라인 완료" 로그 존재 |
| `running` | 🔄 적재 중 | 미완료 + 마지막 로그 15분 이내 |
| `stuck` | 🛑 응답 없음 | 미완료 + 마지막 로그 15분 초과 |
| `no_run` | 📭 실행 없음 | 오늘 시작 로그 없음 |
| `no_log` | 📭 로그 없음 | 로그 파일 없음 |

- `running` / `stuck` 상태 시: 마지막 로그 메시지 + 경과 시간 박스로 표시
- `STUCK_THRESHOLD_MIN = 15` (파일 상단에서 조정 가능)
  - 근거: wave UPDATE 최대 소요 12분 → 15분 임계값으로 오탐 방지

**② 로그 섹션 부분 새로고침 (`@st.fragment`)**
- `_render_log_section()` 함수를 `@st.fragment`로 분리
- 섹션 우측 🔄 버튼 클릭 시 로그 섹션만 재실행 (전체 페이지 재로드 없음)
- `st.rerun(scope="fragment")` 사용 (Streamlit 1.33+ 기능)

**③ 커버리지 달력 적재 완료 시각 표시**
- `get_coverage()`: `loaded_at` 컬럼 추가 조회
- `pipeline_coverage.loaded_at` (UTC) → KST 변환 후 `HH:MM` 형식으로 셀에 표시
- 예: `✅ 10:23` / `✅ 10:23/🔵` (재분석+예보 공존 시)
- 예보(🔵)·누락(❌) 셀은 시각 미표시

---

### ✅ 완료된 작업 (Phase 17, 2026-04-13)

#### Phase 17: 다운로드/적재 상태 분리 + 자동 재시도 (STEP 5) ✅

**배경 및 문제:**
- 기존 `pipeline_coverage.status` 컬럼은 다운로드 실패와 적재 실패를 구분 불가
  - 다운로드 실패인데 적재를 재시도 → 파일 없음으로 실패 반복
  - 적재 실패인데 다운로드를 재시도 → 불필요한 CDS API/OPeNDAP 요청 낭비
- 4월 3-5일 ECMWF wave 적재 실패: TimescaleDB 압축 청크에 UPDATE 시도 → `tuple decompression limit exceeded` 에러
- STEP 1~4 실행 후 누락된 항목을 자동으로 재시도하는 로직 부재

**Stage 1 — `db/schema.py`: 컬럼 추가 마이그레이션**
- `pipeline_coverage` 테이블에 두 컬럼 추가 (멱등 마이그레이션):
  - `download_status`: `complete` / `failed` / `skipped` / `NULL`(미시도)
  - `load_status`: `complete` / `partial` / `failed` / `skipped` / `NULL`(미시도)
- `migrate_coverage_v2()` 함수 신규 — `ADD COLUMN IF NOT EXISTS` 방식으로 멱등성 보장
- `initialize_schema()` 16단계로 자동 호출

**Stage 2 — `db/coverage.py`: 함수 확장**
- `update_coverage()`: `download_status` / `load_status` 파라미터 추가 (하위 호환 유지)
- `get_retry_targets()` 신규: STEP 5 재시도 대상 분류 함수
  - `'download'` → `download_status IS NULL or failed` : 다운로드부터 재시도
  - `'load_only'` → `download_status=complete AND load_status IN (NULL, partial, failed)` : 적재만 재시도
  - 레거시 레코드(두 컬럼 NULL) → `status` 기반으로 `'download'` 분류

**Stage 3 — `db/loader.py`: TimescaleDB 압축 청크 자동 해제**
- `_decompress_chunks_for_update()` 신규 — Strategy B wave UPDATE 직전 호출
- `timescaledb_information.chunks` 뷰에서 대상 날짜·테이블의 압축 청크 조회
- **별도 autocommit 연결**로 `decompress_chunk()` 실행 (DDL이므로 트랜잭션 롤백 불가)
- `if_compressed => TRUE` 옵션으로 이미 해제된 청크 재시도 시 에러 방지 (멱등성)
- 해제된 청크는 TimescaleDB 자동 압축 정책에 의해 주기적으로 재압축됨

**Stage 4 — `pipeline.py`: 단계별 상태 기록**
- ECMWF ERA5 다운로드 루프: `download_day()` 반환값(Path/None)으로 `download_status` 기록
- HYCOM 분석 다운로드 루프: 동일
- `_load_ecmwf_day_to_db()`: 적재 결과에 따라 `load_status` 기록
- `_load_hycom_day_to_db()`: 적재 결과에 따라 `load_status` 기록 (파일 없음 시 `LOAD_SKIPPED`)
- 예보 파이프라인: 파일 없음 → `DL_FAILED + LOAD_SKIPPED`, 파일 있음 → `DL_COMPLETE + LOAD_*`

**Stage 5 — `pipeline.py`: STEP 5 자동 재시도 패스**
- 위치: 예보 파이프라인 완료 직후 (재분석·예보 모두 처리 후)
- `get_retry_targets()` 호출 → 재시도 대상 분류
- `'download'` 타입: 다운로더 재실행 → 파일 획득 → DB 적재
- `'load_only'` 타입: 로컬 파일 재사용 → DB 적재만 수행
- 예보 소스는 재시도 제외 (당일 발행분 아닌 경우 재적재 의미 없음)
- 조건: `need_db=True AND dry_run=False AND mode not in download_only/forecast_download_only`

**Stage 6 — `monitoring/app.py`: 달력 및 재시도 섹션 개선**
- `get_coverage()`: `download_status`, `load_status` 컬럼 추가 조회
- 달력 셀 이모지 세분화:

  | 상태 | 이모지 | 조건 |
  |------|--------|------|
  | 다운+적재 완료 | `✅ HH:MM` | `dl=complete, ld=complete` |
  | 적재 부분 | `⚠️` | `dl=complete, ld=partial` |
  | 다운 완료, 적재 실패 | `📥🔴` | `dl=complete, ld=failed` |
  | 다운로드 실패 | `🔴` | `dl=failed` |
  | 건너뜀 | `⏭️` | `dl=skipped` |
  | 레거시 레코드 | 기존 status 이모지 | `dl=NULL` |

- "🔁 재시도 필요 항목 (STEP 5)" 섹션 신규:
  - 최근 14일 미완료 항목 테이블 표시
  - `download` 타입: 빨간 배경 / `load_only` 타입: 노란 배경

---

### 중기
- [ ] **ERA5T 명시적 수집**: CDS API `product_type` 파라미터 조정 검토
- [ ] **Spec 문서**: `specs/hycom_current.md`, `specs/noaa_forecast.md` 작성
- [x] ~~**ECMWF wave 컬럼 정리**~~: Phase 9에서 완료 (2026-03-17)

### 장기
- [ ] **과거 전체 데이터**: 2021-01-01 ~ 현재 일괄 적재 (`--manual` 옵션 활용)
- [ ] **서버 이전**: 로컬 → 운영 서버 (Linux 환경 — 코드 변경 없이 동작)
- [ ] **Ship Position JOIN 로직**: `floor("3h")` HYCOM 시각 매핑

---

## 10. 주요 기술 결정 사항

| 결정 사항 | 선택한 방식 | 이유 |
|-----------|------------|------|
| ERA5 다운로드 단위 | 1일 1파일 (wind/wave 분리) | API 호출 수 48배 감소 |
| HYCOM 해상도 조절 | stride=3 | ERA5 0.25°와 유사한 0.24° |
| HYCOM 경도 변환 | 0~360 → -180~180 | ECMWF와 좌표계 통일 |
| DB 적재 방식 | PostgreSQL COPY (청크 스트리밍, 500K행/청크) | INSERT 대비 10~50배 빠름, 단일 버퍼 OOM 방지 |
| wind 적재 전략 | Strategy A: 직접 COPY to 메인 테이블 | INSERT ON CONFLICT 대비 훨씬 빠름 |
| wave 적재 전략 | Strategy B: COPY → tmp_load → UPDATE | wind 행이 먼저 있어야 UPDATE 가능한 구조 |
| work_mem 튜닝 | SET LOCAL work_mem = '2GB' (wave UPDATE 시) | 24.9M행 JOIN 해시 테이블 디스크 spill 방지 |
| 테이블 설계 | wind+wave 통합 테이블 | PRIMARY KEY 중복 방지 |
| 시간 기준 | 모두 UTC (TIMESTAMPTZ) | 국제 표준 |
| 파일 자동 감지 | 파일명 패턴 | 하드코딩 제거, 확장성 |
| 재분석 공백 처리 | 예보 임시 커버 → 재분석 제공 시 자동 교체 | 데이터 연속성 + 단일 진실 원칙 보장 |
| ERA5T → ERA5 교체 | UPSERT (Phase 10-C) | ERA5T 적재 후 ERA5 최종본 자동 갱신 |
| 파이프라인 상태 추적 | pipeline_coverage 테이블 (Phase 10) | 누락·부분적재·HYCOM 소실 자동 감지 |
| ERA5 가용 날짜 조회 | CDS API 실제 조회 (고정 -7일 폐기) | 제공업체 지연 가변적이므로 맹목적 가정 금지 |
| 테스트 전략 | --simulate-date + --dry-run | 1일 주기 로직을 분 단위로 빠르게 검증 가능 |
| 예보 바람 출처 | ECMWF Open Data (ecmwf-opendata) | ERA5와 동일 변수, 10일 예보 |
| 예보 파랑(3개) 출처 | ECMWF Open Data | swh/mwd/mwp만 무료 제공 |
| 예보 파랑(9개) 출처 | **NOAA WW3 PacIOOS** (별도 테이블) | ECMWF 미제공 너울·풍파 6개 보완 |
| NOAA ERDDAP 서버 선택 | PacIOOS (pae-paha.pacioos.hawaii.edu) | CoastWatch OPeNDAP 불안정 → PacIOOS 채택 |
| NOAA timedelta 버그 처리 | units="wave_seconds" + decode_timedelta=False | xarray가 units="seconds" → timedelta64 자동 해석하는 버그 우회 |
| NOAA 파랑 DB 설계 | env_noaa_forecast 별도 테이블 (Option A) | ECMWF/NOAA 해상도·시간 간격 다름, 독립 운영 |
| 예보 해류 출처 | HYCOM Forecast OPeNDAP (A안) | 기존 코드 재사용, 추가 가입 불필요 |
| 예보 DB 설계 | issued_at 컬럼 추가 | 동일 시각 예보가 매일 갱신되므로 발행일 구분 필요 |
| 갱신 주기 | 1일 1회 | 운영 단순성 |
| Wind 해양 필터링 | wave NaN 마스크 기준 inner join (Phase 12) | 별도 마스크 파일 불필요, wave 파일과 항상 세트 적재되므로 의존성 없음 |
| Wind 육지 데이터 처리 | 기존 2026-03-05 재적재 안 함, 이후 분부터 필터링 | 1일치 불일치는 허용 (재적재 비용 대비 실익 없음) |
| HYCOM 단독 적재 모드 | `--mode load_hycom_only` 신규 추가 (Phase 13) | `load_only`는 ECMWF까지 전체 적재 → HYCOM 단독 테스트·백필 시 불필요한 작업 방지 |
| 재분석 complete 판정 기준 | 에러 없음 = complete (Phase 14-A 예정) | 행 수 비교는 해양 필터링·HYCOM 실제 격자 수와 맞지 않아 항상 partial 판정 → 에러 기반으로 변경 |
| 예보 테이블 PK 재설계 | `(datetime, lat, lon)` + issued_at 일반 컬럼 (Phase 14-B 예정) | 기존 `(issued_at, datetime, lat, lon)` PK는 매일 쌓이기만 함 → 최신 예보로 덮어쓰는 UPSERT 구조로 변경 |
| 예보 적재 이중 버전 관리 | 최신 issued_at 기준 덮어쓰기, 구버전 자동 삭제 | 예보는 "현재 가장 좋은 예측"만 필요 — 과거 발행본 보관 불필요 |
| 예보 coverage 기록 | `pipeline_coverage`에 `forecast_only` 상태 기록 (Phase 14-C 예정) | 예보 적재 누락을 대시보드에서 감지 가능하도록 |
| HYCOM 영구 손실 복구 | 인근 날짜 데이터 복제 정책 (Phase 15-B 대시보드 연계) | 롤링 윈도우(D-10) 초과 손실 시 공백보다 인근 복제가 LLM 참조 품질에서 나음 |
| 관리자 대시보드 | Streamlit (Phase 15-B 완료) | Python 단독 실행, 별도 프론트엔드 불필요, pipeline_coverage 테이블 직접 조회 |
| 운영 스케줄러 | Windows Task Scheduler + WSL (Phase 15-A 완료) | 매일 10:00 KST 자동 실행 (`--mode auto`) |
| HYCOM 예보 기간 | 5일 유지 (확장 계획 없음) | HYCOM 영구 손실 복구는 인근 날짜 복제 정책으로 처리 — 예보 연장 불필요 |

---

## 11. 알려진 제약사항 및 주의사항

### ERA5 데이터 지연
- ERA5 재분석: 현재 기준 약 **5~8일 지연** 제공 (ECMWF 사정에 따라 가변)
- ERA5T (근실시간): 약 **2~3일 지연** — CDS API에서 동일하게 접근 가능
- Phase 10부터 고정 -7일 가정 폐기 → CDS API 실제 가용 날짜 조회로 변경
- Phase 9까지: `down.json`의 `end_date`를 이 기준 이후로 설정하면 CDS API 오류 발생

### ECMWF Open Data vs CDS API (중요)
- 재분석(ERA5/ERA5T): `cdsapi` 라이브러리 사용
- 예보(HRES): `ecmwf-opendata` 라이브러리 사용 → **완전히 다른 라이브러리**
- 두 가지를 함께 사용하는 구조로 개발 예정

### HYCOM 분석 vs 예보 URL 차이 (2026-03-16 변경)
- ~~분석(구): `GLBy0.08/expt_93.0/uv3z` → 2024-09-05 서비스 종료, 더 이상 사용 불가~~
- 분석·예보(현재): `FMRC_ESPC-D-V02_uv3z/FMRC_ESPC-D-V02_uv3z_best.ncd#fillmismatch`
  - 분석(`hycom_downloader.py`)과 예보(`hycom_forecast_downloader.py`) 모두 동일 URL 사용
  - `_best.ncd`: FMRC 컬렉션에서 각 시각의 최신 run 자동 선택
  - **보유 기간**: 약 D-10일 ~ D+5일 (롤링 윈도우)
  - 두 URL 모두 `#fillmismatch` suffix 필수 (HYCOM 서버 알려진 버그 우회)

### HYCOM URL `#fillmismatch`
- 없으면 `_FillValue` 타입 불일치 오류 발생 (HYCOM 서버 알려진 버그)

### 예보 테이블 issued_at 컬럼
- 같은 미래 시각의 예보가 매일 갱신되므로 반드시 발행일 구분 필요
- LLM Agent 조회 시 `MAX(issued_at)` 기준으로 최신 예보만 선택

### HYCOM 시간 해상도
- HYCOM: 3시간 간격 / ERA5: 1시간 간격
- LLM Agent에서 JOIN 시 `floor("3h")` 처리 필요

### TimescaleDB 압축 정책
- 30일 이상 청크 자동 압축 → 압축된 청크는 UPDATE/DELETE 불가
- Phase 17부터 Strategy B(wave UPDATE) 실행 전 `_decompress_chunks_for_update()` 자동 호출
- `decompress_chunk()` 완료 후 청크는 자동 압축 정책에 의해 주기적으로 재압축됨
- `decompress_chunk(chunk, if_compressed => TRUE)` : 이미 해제된 청크 재실행 시 에러 없이 건너뜀

### NOAA WW3 제약사항 (2026-03-16 확인)
- **공간 범위**: 위도 -77.5° ~ 77.5° (극지방 미포함)
- **해상도**: 0.5° (ECMWF 0.25°, HYCOM ~0.24°보다 낮음)
- **시간 간격**: 1시간 (ECMWF 6시간보다 세밀)
- **예보 기간**: 최대 약 7일 (PacIOOS 서버 보유 기간)
- **파일 크기**: 약 369 MB/일 (48 스텝 × 311×720 격자점 × 9변수)
- `sper`/`wper` 변수: `units="seconds"` → xarray timedelta64 오해석 → 저장 시 `units="wave_seconds"`, 읽기 시 `decode_timedelta=False` 필수

### DB 용량 추정 (2026-03-21 측정 기준)
- **1일치 (비압축)**: ~7.2 GB (env_ecmwf_reanalysis, wind+wave)
- **8일치**: ~58 GB / **1년**: ~2.6 TB / **전체(2021~현재 5.2년)**: ~13 TB
- **TimescaleDB 압축 후**: 5~10배 감소 예상 (~1.3~2.6 TB)
- **현재 디스크**: Windows C드라이브 931G (사용 253G, 여유 **678G**) — 8일치 적재 가능
- **압축 정책**: 30일 이상 청크 자동 압축 (schema.py 설정) → 압축 후 재적재 시 `decompress_chunk()` 필요

### wave UPDATE 성능 제약 (2026-03-21 확인)
- wave 파일 1일치: **21.9분** (COPY 8분 + 인덱스 30초 + UPDATE 11.6분)
- work_mem=2GB 적용 중 — 메모리 여유 없을 경우 disk spill로 더 느려질 수 있음
- wind 파일이 먼저 적재되어야 wave UPDATE 가능 (wind 행이 없으면 UPDATE 0건)

### ECMWF wave 컬럼 제거 완료 (2026-03-17)
- `env_ecmwf_forecast`의 파랑 9개 컬럼 완전 제거, u10/v10 바람 전용 테이블로 확정
- 파랑 예보(9개 변수)는 `env_noaa_forecast` 테이블에서 전담
- LLM Agent 개발 시: 바람 → `env_ecmwf_forecast`, 파랑 → `env_noaa_forecast`, 해류 → `env_hycom_forecast`

### EXPECTED_ROWS 불일치 (Phase 14-A 에서 수정 예정)
- `coverage.py`의 `EXPECTED_ROWS` 상수가 실제 적재 행 수와 맞지 않음
  - `SOURCE_ECMWF_REANALYSIS = 24_917_760` → 해양 필터링 후 실제 ≈ 13.8M (55%)
  - `SOURCE_HYCOM_CURRENT = 12_008_008` → 실제 17,004,000
- 현재 결과: 모든 날짜가 `partial` 판정 → `cleanup_superseded_forecasts()` 미실행 → 예보 데이터 누적
- Phase 14-A에서 에러 기반 판정으로 변경 예정

### 예보 데이터 관리 정책 (Phase 14-B 변경 후)
- 예보 테이블 PK 변경 전 (`issued_at, datetime, lat, lon`): 매일 새 행이 추가되어 구버전 삭제 안 됨
- 예보 테이블 PK 변경 후 (`datetime, lat, lon`): 같은 시각에 최신 예보로 UPSERT, 자동 교체
- LLM Agent 쿼리: 변경 후에는 단순 `SELECT` 로 항상 최신 예보 조회 가능 (`MAX(issued_at)` 불필요)

### HYCOM 롤링 윈도우 영구 손실 리스크
- HYCOM 분석 OPeNDAP는 약 D-10일 ~ D+5일의 롤링 윈도우만 보유
- D-10 이전 날짜는 OPeNDAP에서 삭제됨 → 해당 날짜 다운로드 미완료 시 영구 손실
- 손실 감지: `pipeline_coverage`의 `permanent_forecast` 상태 (check_and_promote_hycom_permanent 함수)
- 손실 복구 정책: 인근 날짜 데이터 복제 (`INSERT ... SELECT` 방식) — Phase 15-B 대시보드에서 안내

---

## 12. GitHub 저장소

- **URL**: https://github.com/Melanius/weather_pipeline
- **브랜치**: `main`
- **포함 내용**: 전체 소스코드, 설정 파일, 문서, 스크립트
- **제외 항목**: `data/` (NetCDF 파일), `logs/`, `.env` (API 키/DB 비밀번호)

| 커밋 | 날짜 | 내용 |
|------|------|------|
| `b634a15` | 2026-03-16 | feat: 환경 데이터 수집 파이프라인 초기 구축 (36개 파일) |
| `c12e947` | 2026-03-16 | docs: Project Plan Phase 8 내용 반영 (NOAA WW3 파이프라인) |
| `afc8d35` | 2026-03-17 | refactor: ECMWF 예보 파랑 수집 제거 — NOAA WW3로 전담 |
| `55b9c0a` | 2026-03-17 | docs: Project Plan Phase 9 반영 (ECMWF 파랑 수집 제거 완료) |
| `0c054b2` | 2026-03-18 | chore: pyproject.toml Windows x64 환경 지원 추가 |
| `2d4b1e8` | 2026-03-18 | docs: Project Plan Windows 환경 지원 내용 반영 |
| (미커밋) | 2026-03-18~21 | feat: Phase 10 파이프라인 상태관리 + Phase 11 DB 성능 최적화 |
| (미커밋) | 2026-04-04 | feat: Phase 12 Wind 해양 격자 필터링 (loader.py 3.6단계 추가) |
| (미커밋) | 2026-04-04 | feat: Phase 13 load_hycom_only 모드 추가 + Step 6 HYCOM 적재 완료 |
| (미커밋) | 2026-04-05 | feat: Phase 14 예보 파이프라인 DB 적재 완료 (PK 재설계·coverage 기록·버그 5종 수정) |
| (미커밋) | 2026-04-05 | feat: Phase 15-A 스케줄러 설정 + Phase 15-B Streamlit 모니터링 대시보드 |
| (미커밋) | 2026-04-05 | docs: Project Plan Phase 14~15-C 완료 반영 |
| (미커밋) | 2026-04-12 | feat: Phase 16 모니터링 대시보드 개선 (실행 상태 판정 개선·로그 부분 새로고침·커버리지 시각 표시) |
| (미커밋) | 2026-04-13 | feat: Phase 17 다운로드/적재 상태 분리 + TimescaleDB 압축 청크 자동 해제 + STEP 5 자동 재시도 |
