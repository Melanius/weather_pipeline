# llm_for_ship — 환경 데이터 파이프라인 개발 기록

> **프로젝트**: 한화오션 × 동국대 산학협력 — 선박 특화 LLM 개발 (2026년 과제)
> **담당자**: 이훈정 책임 (한화오션)
> **담당 범위**: 환경 데이터 수집 → PostgreSQL(TimescaleDB) 적재 파이프라인
> **작업 환경**: Windows 11 + WSL2 (Ubuntu) / Python 3.12 / UV / Docker Desktop
> **최초 작성일**: 2026-03-15 (KST) | **최종 수정일**: 2026-03-17 (KST, Phase 9 완료)

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

### 1-2. 수집 데이터 종류 — 예보(미래) ✅ 개발 완료 (DB 적재 테스트 미완료)

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

### 7-2. 추가된 테이블 (예보) ✅ 구현 완료 (DB 적재 테스트 미완료)

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

### 8-2. 예보 파이프라인 (✅ 다운로드 완료, DB 적재 테스트 미완료)
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

### ⭐ 즉시 (다음 세션 첫 번째 작업)

#### Step 5. DB 적재 테스트
- [ ] Docker Desktop 기동 → `uv run python run.py --init-db`
  - 5개 테이블 정상 생성 확인:
    - `env_ecmwf_reanalysis` (바람+파랑 통합)
    - `env_hycom_current` (해류 분석)
    - `env_ecmwf_forecast` (바람만 — u10/v10)  ← Phase 9 변경 반영
    - `env_hycom_forecast` (해류 예보)
    - `env_noaa_forecast` (파랑 예보 9개)
- [ ] `down.json` 날짜 안전 범위 재설정 후 재분석 다운로드 재확인
  - `{"start_date": "2026-03-01", "end_date": "2026-03-09"}`
- [ ] `uv run python run.py --mode load_only` → 재분석 DB 적재 확인
- [ ] `uv run python run.py --mode forecast` → 예보 DB 적재 확인 (ECMWF 바람 + HYCOM + NOAA)

### 중기
- [ ] **스케줄러**: 매일 오전 8시 KST 자동 실행 (APScheduler 또는 cron)
- [x] ~~**ECMWF wave 컬럼 정리**~~: Phase 9에서 완료 (2026-03-17)
- [ ] **ERA5T 명시적 수집**: CDS API `product_type` 파라미터 조정 검토
- [ ] **Spec 문서**: `specs/hycom_current.md`, `specs/noaa_forecast.md` 작성

### 장기
- [ ] **과거 전체 데이터**: 2021-01-01 ~ 현재 일괄 적재
- [ ] **서버 이전**: 로컬 → 운영 서버
- [ ] **Ship Position JOIN 로직**: `floor("3h")` HYCOM 시각 매핑
- [ ] **UPSERT 전환**: ERA5T → ERA5 덮어쓰기 (현재는 ON CONFLICT DO NOTHING)

---

## 10. 주요 기술 결정 사항

| 결정 사항 | 선택한 방식 | 이유 |
|-----------|------------|------|
| ERA5 다운로드 단위 | 1일 1파일 (wind/wave 분리) | API 호출 수 48배 감소 |
| HYCOM 해상도 조절 | stride=3 | ERA5 0.25°와 유사한 0.24° |
| HYCOM 경도 변환 | 0~360 → -180~180 | ECMWF와 좌표계 통일 |
| DB 적재 방식 | PostgreSQL COPY | INSERT 대비 10~50배 빠름 |
| 테이블 설계 | wind+wave 통합 테이블 | PRIMARY KEY 중복 방지 |
| 시간 기준 | 모두 UTC (TIMESTAMPTZ) | 국제 표준 |
| 파일 자동 감지 | 파일명 패턴 | 하드코딩 제거, 확장성 |
| 재분석 공백 처리 | B안+C안 (ERA5T + 예보 누적) | 개발 공수 최소, 5일 후 공백 자동 해소 |
| ERA5T → ERA5 교체 | 1차: ON CONFLICT DO NOTHING | 품질 차이 미미, UPSERT는 추후 선택적 추가 |
| 예보 바람 출처 | ECMWF Open Data (ecmwf-opendata) | ERA5와 동일 변수, 10일 예보 |
| 예보 파랑(3개) 출처 | ECMWF Open Data | swh/mwd/mwp만 무료 제공 |
| 예보 파랑(9개) 출처 | **NOAA WW3 PacIOOS** (별도 테이블) | ECMWF 미제공 너울·풍파 6개 보완 |
| NOAA ERDDAP 서버 선택 | PacIOOS (pae-paha.pacioos.hawaii.edu) | CoastWatch OPeNDAP 불안정 → PacIOOS 채택 |
| NOAA timedelta 버그 처리 | units="wave_seconds" + decode_timedelta=False | xarray가 units="seconds" → timedelta64 자동 해석하는 버그 우회 |
| NOAA 파랑 DB 설계 | env_noaa_forecast 별도 테이블 (Option A) | ECMWF/NOAA 해상도·시간 간격 다름, 독립 운영 |
| 예보 해류 출처 | HYCOM Forecast OPeNDAP (A안) | 기존 코드 재사용, 추가 가입 불필요 |
| 예보 DB 설계 | issued_at 컬럼 추가 | 동일 시각 예보가 매일 갱신되므로 발행일 구분 필요 |
| 갱신 주기 | 1일 1회 | 운영 단순성 |

---

## 11. 알려진 제약사항 및 주의사항

### ERA5 데이터 지연
- ERA5 재분석: 현재 기준 약 **5일 지연** 제공
- ERA5T (근실시간): 약 **2~3일 지연** — CDS API에서 동일하게 접근 가능
- `down.json`의 `end_date`를 이 기준 이후로 설정하면 CDS API 오류 발생

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
- 30일 이상 청크 자동 압축 → 압축된 청크는 쓰기 불가
- 과거 데이터 재적재 시 `SELECT decompress_chunk(...)` 필요

### NOAA WW3 제약사항 (2026-03-16 확인)
- **공간 범위**: 위도 -77.5° ~ 77.5° (극지방 미포함)
- **해상도**: 0.5° (ECMWF 0.25°, HYCOM ~0.24°보다 낮음)
- **시간 간격**: 1시간 (ECMWF 6시간보다 세밀)
- **예보 기간**: 최대 약 7일 (PacIOOS 서버 보유 기간)
- **파일 크기**: 약 369 MB/일 (48 스텝 × 311×720 격자점 × 9변수)
- `sper`/`wper` 변수: `units="seconds"` → xarray timedelta64 오해석 → 저장 시 `units="wave_seconds"`, 읽기 시 `decode_timedelta=False` 필수

### ECMWF wave 컬럼 제거 완료 (2026-03-17)
- `env_ecmwf_forecast`의 파랑 9개 컬럼 완전 제거, u10/v10 바람 전용 테이블로 확정
- 파랑 예보(9개 변수)는 `env_noaa_forecast` 테이블에서 전담
- LLM Agent 개발 시: 바람 → `env_ecmwf_forecast`, 파랑 → `env_noaa_forecast`, 해류 → `env_hycom_forecast`

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
