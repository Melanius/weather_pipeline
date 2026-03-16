# ECMWF 예보 데이터 수집 명세

## 1. 데이터 출처

| 항목 | 내용 |
|------|------|
| 모델 | ECMWF HRES (High-Resolution Forecast) |
| 접근 방법 | `ecmwf-opendata` Python 라이브러리 (CDS API와 별개) |
| 인증 | 불필요 (공개 데이터) |
| 데이터 형식 | GRIB2 다운로드 → cfgrib 변환 → NetCDF 저장 |

## 2. 수집 변수

### 바람 (Wind)
| 변수 | 설명 | 단위 |
|------|------|------|
| u10 | 10m 동서 풍속 | m/s |
| v10 | 10m 남북 풍속 | m/s |

### 파랑 (Wave)
| 변수 | 설명 | 단위 |
|------|------|------|
| swh | 복합 유의파고 | m |
| mwd | 평균 파랑 방향 | ° |
| mwp | 평균 파랑 주기 | s |
| shts | 너울 유의파고 | m |
| mdts | 너울 방향 | ° |
| mpts | 너울 주기 | s |
| shww | 풍파 유의파고 | m |
| mdww | 풍파 방향 | ° |
| mpww | 풍파 주기 | s |

> ERA5 재분석과 동일한 변수 목록 → DB 스키마 재사용 가능

## 3. 시공간 범위

| 항목 | 내용 |
|------|------|
| 예보 기간 | 발행일 기준 10일 (0~240시간) |
| 시간 간격 | 6시간 (0, 6, 12, ..., 240시 → 41 스텝) |
| 공간 해상도 | 0.25° (전세계) |
| 위도 범위 | -90° ~ 90° |
| 경도 범위 | -180° ~ 180° (0~360 → -180~180 변환 적용) |

## 4. 갱신 주기

- 1일 1회 (매일 apm 8시 KST = UTC 23:00 실행 예정)
- 최신 발행 예보 기준 (00 UTC 또는 12 UTC run 중 최신)

## 5. 저장 경로 및 파일명

```
data/ecmwf/forecast/YYYY/MM/
  ecmwf_fc_wind_YYYYMMDD.nc   ← YYYYMMDD = 예보 발행일
  ecmwf_fc_wave_YYYYMMDD.nc
```

- NetCDF 전역 속성에 `issued_at` (발행 시각, ISO 형식) 저장

## 6. DB 적재 테이블

`env_ecmwf_forecast` 테이블 (재분석 테이블에 `issued_at` 컬럼 추가)

- PRIMARY KEY: (issued_at, datetime, lat, lon)
- 동일 시각 예보가 매일 갱신되므로 issued_at으로 구분 필수

## 7. 주의사항

- `ecmwf-opendata`는 GRIB2 형식 다운로드 → `cfgrib` + `eccodes` 추가 설치 필요
- GRIB2 경도: 0~360 → -180~180 변환 필요 (ERA5와 좌표계 통일)
- 파랑 변수 중 너울 성분(shts, mdts, mpts)이 ECMWF Open Data 무료 제공 범위에 포함되지 않을 수 있음 → 미포함 시 NaN으로 저장
