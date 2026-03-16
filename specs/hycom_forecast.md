# HYCOM 예보 해류 데이터 수집 명세

## 1. 데이터 출처

| 항목 | 내용 |
|------|------|
| 모델 | HYCOM ESPC-D (Global Ocean Forecast) |
| 접근 방법 | OPeNDAP URL 스트리밍 (기존 분석과 동일 방식) |
| 인증 | 불필요 |
| URL | `https://tds.hycom.org/thredds/dodsC/FMRC_ESPC-D-V02_uv3z/FMRC_ESPC-D-V02_uv3z_best.ncd#fillmismatch` |

> `#fillmismatch`: HYCOM 서버의 _FillValue 타입 불일치 버그 우회 (분석과 동일)
> `_best.ncd`: 각 시각에 대해 최신 예보를 자동 선택하는 Best Time Series

## 2. 수집 변수

| 변수 | 설명 | 단위 |
|------|------|------|
| water_u | 동서 해류 속도 (수심 0m) | m/s |
| water_v | 남북 해류 속도 (수심 0m) | m/s |

> HYCOM 분석(reanalysis)과 동일한 변수 → DB 구조 동일

## 3. 시공간 범위

| 항목 | 내용 |
|------|------|
| 예보 기간 | 오늘 00:00 UTC ~ 5일 후 (120시간) |
| 시간 간격 | 3시간 (00, 03, 06, ..., 21시) |
| 공간 해상도 | stride=3 적용 → 약 0.24° (분석과 동일) |
| 수심 | 0m (해수면, isel(depth=0)) |
| 경도 | 0~360 → -180~180 변환 적용 |

## 4. 갱신 주기

- 1일 1회 (ECMWF 예보와 동시 실행)

## 5. 저장 경로 및 파일명

```
data/hycom/forecast/YYYY/MM/
  hycom_fc_current_YYYYMMDD.nc   ← YYYYMMDD = 다운로드 실행일 (발행일 기준)
```

- NetCDF 전역 속성에 `issued_at` 저장

## 6. DB 적재 테이블

`env_hycom_forecast` 테이블 (분석 테이블에 `issued_at` 컬럼 추가)

- PRIMARY KEY: (issued_at, datetime, lat, lon)

## 7. 주의사항

- 분석 URL(`expt_93.0`)과 예보 URL(`FMRC_ESPC-D-V02`)이 다름
- `_best.ncd`는 FMRC (Forecast Model Run Collection)의 최신 예보 시계열
- HYCOM 예보 기간은 5일로 ECMWF(10일)보다 짧음 → 향후 연장 필요 시 CMEMS 검토
- 공백 기간 해류(D-1~D): 기존 분석 HYCOM이 약 1일 지연으로 자동 커버
