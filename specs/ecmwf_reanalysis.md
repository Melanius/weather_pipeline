# ECMWF ERA5 재분석 데이터 명세서

## 1. 데이터 소스

| 항목 | 내용 |
|------|------|
| 데이터셋 | ERA5 (ECMWF Reanalysis v5) |
| API | CDS (Climate Data Store) API |
| URL | https://cds.climate.copernicus.eu/api |
| Python 라이브러리 | `cdsapi` |
| 데이터 타입 | 과거 재분석 (Reanalysis) |
| 시간 범위 | 1940년~현재 (본 프로젝트: 2021-01-01~) |
| 공간 범위 | 전 세계 |
| 시간 해상도 | 1시간 간격 |
| 공간 해상도 | 0.25° × 0.25° (원본) |

## 2. 수집 변수 목록

### 파랑 (Wave) 변수

| 단축명 | CDS 변수명 | 설명 | 단위 |
|--------|-----------|------|------|
| swh | significant_height_of_combined_wind_waves_and_swell | 복합 유의파고 (풍파+너울) | m |
| mwd | mean_wave_direction | 평균 파랑 진행 방향 | ° (0=북, 시계방향) |
| mwp | mean_wave_period | 평균 파랑 주기 | s |
| shts | significant_height_of_total_swell | 너울 유의파고 | m |
| mdts | mean_direction_of_total_swell | 너울 진행 방향 | ° |
| mpts | mean_period_of_total_swell | 너울 주기 | s |
| shww | significant_height_of_wind_waves | 풍파 유의파고 | m |
| mdww | mean_direction_of_wind_waves | 풍파 진행 방향 | ° |
| mpww | mean_period_of_wind_waves | 풍파 주기 | s |

### 바람 (Wind) 변수

| 단축명 | CDS 변수명 | 설명 | 단위 |
|--------|-----------|------|------|
| u10 | 10m_u_component_of_wind | 10m 고도 동서 방향 풍속 (동쪽 +) | m/s |
| v10 | 10m_v_component_of_wind | 10m 고도 남북 방향 풍속 (북쪽 +) | m/s |

## 3. 데이터 흐름

```
CDS API 요청
    └─ dataset: reanalysis-era5-single-levels
    └─ variables: 11개 (wave 9개 + wind 2개)
    └─ 시간: 하루 24시간 (00:00 ~ 23:00)
    └─ format: netcdf
         ↓
NetCDF 파일 저장
    └─ 경로: data/ecmwf/reanalysis/YYYY/MM/YYYYMMDD.nc
    └─ 크기: 약 100MB/일 (0.25°) / 약 7MB/일 (1°)
         ↓
xarray로 파일 파싱
    └─ 좌표: time × latitude × longitude
    └─ 변수: 11개
         ↓
Pandas DataFrame 변환 (행: 각 격자점/시간 조합)
         ↓
PostgreSQL TimescaleDB 적재 (COPY 방식)
    └─ 테이블: env_ecmwf_reanalysis
```

## 4. API 요청 파라미터 예시

```python
{
    "product_type": ["reanalysis"],
    "variable": ["significant_height_of_combined_wind_waves_and_swell", ...],
    "year": "2025",
    "month": "02",
    "day": ["14"],
    "time": ["00:00", "01:00", ..., "23:00"],
    "data_format": "netcdf",
    "download_format": "unarchived"
}
```

## 5. 용량 예측

| 해상도 | 격자수 | 1일 용량(NetCDF) | 1월 용량 | 1년 용량 |
|--------|--------|----------------|---------|---------|
| 0.25° | 721×1440 | ~100MB | ~3GB | ~36GB |
| 0.5° | 361×720 | ~25MB | ~750MB | ~9GB |
| 1.0° | 181×360 | ~7MB | ~200MB | ~2.4GB |

> **현재 설정**: 테스트는 1.0°, 운영은 0.25°
